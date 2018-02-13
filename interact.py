#!/usr/bin/env python3
import code
import os
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
SECRETS = "/var/run/secrets/kubernetes.io/serviceaccount/"

namespace = None
try:
    config.load_incluster_config()
    with open(os.path.join(SECRETS, "namespace"), "r") as f:
        namespace = f.read()
except ConfigException:
    config.load_kube_config()
v1 = client.CoreV1Api()
if namespace:
    print("restricting pod list to current namespace '%s'" % namespace)
ret = v1.list_pod_for_all_namespaces(watch=False)
for i in ret.items:
    if namespace:
        if i.metadata.namespace != namespace:
            continue
    print("%s\t%s\t%s" %
          (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
code.interact(local=locals())
