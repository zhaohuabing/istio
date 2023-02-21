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

// ServiceInformer provides access to a shared informer and lister for
// Services.
type ServiceInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1.ServiceLister
}

type FilteredServiceInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewFilteredServiceInformer constructs a new informer for Service type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredServiceInformer(client kubernetes.Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}

				if util.IsNamespaceHosted() {
					serviceList := &corev1.ServiceList{
						Items: []corev1.Service{},
					}
					nsList, err := getManagedNSList(client)
					if err != nil {
						return nil, err
					}
					for _, ns := range nsList.Items {
						singleNsServiceList, err := client.CoreV1().Services(ns.Name).List(context.TODO(), options)
						if err != nil {
							return nil, err
						}
						serviceList.Items = append(serviceList.Items, singleNsServiceList.Items...)
					}
					log.Infof("list length " + strconv.Itoa(len(serviceList.Items)))
					return serviceList, nil
				}
				serviceList, err := client.CoreV1().Services(namespace).List(context.TODO(), options)
				return serviceList, err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}

				w, err := client.CoreV1().Services(namespace).Watch(context.TODO(), options)
				if err != nil {
					return nil, err
				}
				if util.IsNamespaceHosted() {
					nsList, err := getManagedNSList(client)
					if err != nil {
						return nil, err
					}
					return watch.Filter(w, func(in watch.Event) (watch.Event, bool) {
						if service, ok := in.Object.(*corev1.Service); ok {
							for _, ns := range nsList.Items {
								if service.Namespace == ns.Name {
									log.Debugf("watch service:" + service.Namespace + "/" + service.Name)
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
		&corev1.Service{},
		resyncPeriod,
		indexers,
	)
}

func (f *FilteredServiceInformer) defaultInformer(client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredServiceInformer(client, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *FilteredServiceInformer) Informer() cache.SharedIndexInformer {
	log.Infof("create service informer")
	return f.factory.InformerFor(&corev1.Service{}, f.defaultInformer)
}

func (f *FilteredServiceInformer) Lister() v1.ServiceLister {
	return v1.NewServiceLister(f.Informer().GetIndexer())
}
