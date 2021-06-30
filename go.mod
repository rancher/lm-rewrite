module github.com/longhorn/longhorn-manager

go 1.13

replace (
	k8s.io/api => k8s.io/api v0.16.15
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.16.15
	k8s.io/apimachinery => k8s.io/apimachinery v0.16.15
	k8s.io/apiserver => k8s.io/apiserver v0.16.15
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.16.15
	k8s.io/client-go => k8s.io/client-go v0.16.15
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.16.15
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.16.15
	k8s.io/code-generator => k8s.io/code-generator v0.16.16-rc.0
	k8s.io/component-base => k8s.io/component-base v0.16.15
	k8s.io/cri-api => k8s.io/cri-api v0.16.16-rc.0
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.16.15
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.16.15
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.16.15
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.16.15
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.16.15
	k8s.io/kubectl => k8s.io/kubectl v0.16.15
	k8s.io/kubelet => k8s.io/kubelet v0.16.15
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.16.15
	k8s.io/metrics => k8s.io/metrics v0.16.15
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.16.15
)

require (
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/c9s/goprocinfo v0.0.0-20190309065803-0b2ad9ac246b // indirect
	github.com/container-storage-interface/spec v1.1.0
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/docker/go-connections v0.3.0
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/groupcache v0.0.0-20191002201903-404acd9df4cc // indirect
	github.com/golang/protobuf v1.3.3-0.20190920234318-1680a479a2cf
	github.com/google/btree v1.0.0 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.3 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/jinzhu/copier v0.0.0-20190924061706-b57f9002281a
	github.com/kubernetes-csi/csi-lib-utils v0.6.1
	github.com/longhorn/backing-image-manager v0.0.0-20210420131143-904cd772a7a9
	github.com/longhorn/backupstore v0.0.0-20210504063753-dfee2468733e
	github.com/longhorn/go-iscsi-helper v0.0.0-20201111045018-ee87992ec536
	github.com/longhorn/longhorn-instance-manager v0.0.0-20201016215346-d8437b4e156e
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.2.1
	github.com/rancher/go-rancher v0.1.1-0.20190307222549-9756097e5e4c
	github.com/robfig/cron v1.2.0
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/urfave/cli v1.22.1
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	golang.org/x/net v0.0.0-20200707034311-ab3426394381
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0
	google.golang.org/grpc v1.23.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/api v0.16.15
	k8s.io/apiextensions-apiserver v0.0.0
	k8s.io/apimachinery v0.16.15
	k8s.io/client-go v0.16.15
	k8s.io/kubernetes v1.16.15
	k8s.io/metrics v0.16.15
	k8s.io/utils v0.0.0-20190801114015-581e00157fb1
)

replace github.com/longhorn/backupstore => github.com/jenting/longhorn-backupstore v0.0.0-20210712080510-51d6af04f13d
