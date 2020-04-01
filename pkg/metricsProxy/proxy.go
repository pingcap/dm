package metricsProxy

import (
	"crypto/md5"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type Proxy interface {
	GetLabels() map[string]map[string]string
	vecDelete(prometheus.Labels) bool
}

func noteLabels(proxy Proxy, labels map[string]string) {
	labelsMd5 := getLabelsMd5(labels)

	if _, ok := proxy.GetLabels()[labelsMd5]; !ok {
		proxy.GetLabels()[labelsMd5] = labels
	}
}

func getLabelsMd5(labels map[string]string) string {
	var str string
	for _, label := range labels {
		str += label
	}
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}

func findAndDeleteLabels(proxy Proxy, labels prometheus.Labels) bool {
	var (
		deleteLabelsList = make([]map[string]string, 0)
		res              bool
	)
	inputLabelsLen := len(labels)
	for _, ls := range proxy.GetLabels() {
		t := 0
		for k := range labels {
			if ls[k] == labels[k] {
				t++
			}
		}
		if t == inputLabelsLen {
			deleteLabelsList = append(deleteLabelsList, ls)
		}
	}

	for _, deleteLabels := range deleteLabelsList {
		res = res && proxy.vecDelete(deleteLabels)
	}
	return res
}
