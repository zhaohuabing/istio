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

// EndpointsInformer provides access to a shared informer and lister for
type EndpointsInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.EndpointsLister
}

type FilteredEndpointsInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewFilteredEndpointsInformer constructs a new informer for Endpoints type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredEndpointsInformer(client kubernetes.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}

				if util.IsNamespaceHosted() {
					endpointsList := &corev1.EndpointsList{
						Items: []corev1.Endpoints{},
					}
					nsList, err := getManagedNSList(client)
					if err != nil {
						return nil, err
					}
					for _, ns := range nsList.Items {
						singleNsEndpointsList, err := client.CoreV1().Endpoints(ns.Name).List(context.TODO(), options)
						if err != nil {
							return nil, err
						}
						endpointsList.Items = append(endpointsList.Items, singleNsEndpointsList.Items...)
					}
					log.Infof("list length " + strconv.Itoa(len(endpointsList.Items)))
					return endpointsList, nil
				}
				endpointsList, err := client.CoreV1().Endpoints(namespace).List(context.TODO(), options)
				return endpointsList, err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}

				w, err := client.CoreV1().Endpoints(namespace).Watch(context.TODO(), options)
				if err != nil {
					return nil, err
				}
				if util.IsNamespaceHosted() {
					nsList, err := getManagedNSList(client)
					if err != nil {
						return nil, err
					}
					return watch.Filter(w, func(in watch.Event) (watch.Event, bool) {
						if endpoints, ok := in.Object.(*corev1.Endpoints); ok {
							for _, ns := range nsList.Items {
								if endpoints.Namespace == ns.Name {
									log.Debugf("watch endpoints:" + endpoints.Namespace + "/" + endpoints.Name)
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
		&corev1.Endpoints{},
		resyncPeriod,
		indexers,
	)
}

func getManagedNSList(client kubernetes.Interface) (*corev1.NamespaceList, error) {
	nsList, err := client.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{
		LabelSelector: "tcm.cloud.tencent.com/managed-by=" + util.MeshID(),
	})
	allNs := ""
	for _, ns := range nsList.Items {
		allNs = allNs + ns.Name + " "
	}
	log.Infof("all ns:" + allNs)
	return nsList, err
}

func (f *FilteredEndpointsInformer) defaultInformer(client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredEndpointsInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *FilteredEndpointsInformer) Informer() cache.SharedIndexInformer {
	log.Infof("create endpoints informer")
	return f.factory.InformerFor(&corev1.Endpoints{}, f.defaultInformer)
}

func (f *FilteredEndpointsInformer) Lister() v1.EndpointsLister {
	return v1.NewEndpointsLister(f.Informer().GetIndexer())
}
