package kube

import (
	"context"
	"istio.io/istio/pkg/util"
	"istio.io/pkg/log"
	"strconv"
	time "time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	internalinterfaces "k8s.io/client-go/informers/internalinterfaces"
	kubernetes "k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	cache "k8s.io/client-go/tools/cache"
)

// PodInformer provides access to a shared informer and lister for
// Pods.
type PodInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.PodLister
}

type FilteredPodInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewFilteredPodInformer constructs a new informer for Pod type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredPodInformer(client kubernetes.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}

				if util.IsNamespaceHosted() {
					podList := &corev1.PodList{
						Items: []corev1.Pod{},
					}
					nsList, err := getManagedNSList(client)
					if err != nil {
						return nil, err
					}
					for _, ns := range nsList.Items {
						singleNsPodList, err := client.CoreV1().Pods(ns.Name).List(context.TODO(), options)
						if err != nil {
							return nil, err
						}
						podList.Items = append(podList.Items, singleNsPodList.Items...)
					}
					log.Infof("list length " + strconv.Itoa(len(podList.Items)))
					return podList, nil
				}
				podList, err := client.CoreV1().Pods(namespace).List(context.TODO(), options)
				return podList, err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}

				w, err := client.CoreV1().Pods(namespace).Watch(context.TODO(), options)
				if err != nil {
					return nil, err
				}
				if util.IsNamespaceHosted() {
					nsList, err := getManagedNSList(client)
					if err != nil {
						return nil, err
					}
					return watch.Filter(w, func(in watch.Event) (watch.Event, bool) {
						if pod, ok := in.Object.(*corev1.Pod); ok {
							for _, ns := range nsList.Items {
								if pod.Namespace == ns.Name {
									log.Debugf("watch pod:" + pod.Namespace + "/" + pod.Name)
									return in, true
								}
							}
							return in, false
						}
						return in, true
					}), nil
				}
				return w, nil
			},
		},
		&corev1.Pod{},
		resyncPeriod,
		indexers,
	)
}

func (f *FilteredPodInformer) defaultInformer(client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredPodInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *FilteredPodInformer) Informer() cache.SharedIndexInformer {
	log.Infof("create pod informer")
	return f.factory.InformerFor(&corev1.Pod{}, f.defaultInformer)
}

func (f *FilteredPodInformer) Lister() v1.PodLister {
	return v1.NewPodLister(f.Informer().GetIndexer())
}
