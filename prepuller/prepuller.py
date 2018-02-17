import argparse
import json
import logging
import os
import sys
import time
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from .scanrepo import ScanRepo


class Prepuller(object):
    """Class for generating and reaping the Pods for the prepuller.
    """
    repo = None
    logger = None
    client = None
    args = argparse.Namespace(debug=False,
                              json=True,
                              repo=None,
                              owner="lsstsqre",
                              name="jld-lab",
                              port=None,
                              dailies=3,
                              weeklies=2,
                              releases=1,
                              insecure=False,
                              sort="comp_ts",
                              list=None,
                              command=["/bin/sh",
                                       "-c",
                                       "echo Prepuller run for $(hostname)" +
                                       "complete at $(date)."],
                              path="/v2/repositories/lsstsqre/jld-lab/tags/",
                              no_scan=False,
                              namespace=None
                              )
    images = []
    nodes = []
    pod_specs = []
    created_pods = []

    def __init__(self, args=None):
        logging.basicConfig()
        self.logger = logging.getLogger(__name__)
        if args:
            self.args = args
        if self.args and self.args.debug:
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug("Debug logging on.")
        else:
            self.logger.setLevel(logging.INFO)
        namespace = None
        try:
            config.load_incluster_config()
            secrets = "/var/run/secrets/kubernetes.io/serviceaccount/"
            try:
                with open(os.path.join(secrets, "namespace"), "r") as f:
                    namespace = f.read()
            except OSError:
                pass
        except ConfigException:
            try:
                config.load_kube_config()
            except Exception:
                logging.critical(sys.argv[0], " must be run from a system",
                                 " with k8s API access.")
                raise
        if self.args.namespace:
            namespace = self.args.namespace
        if not namespace:
            namespace = os.getenv('JLD_NAMESPACE')
        if not namespace:
            self.logger.warning("Using namespace 'default'")
            namespace = "default"
        self.namespace = namespace
        self.client = client.CoreV1Api()
        self.logger.debug("Arguments: %s" % str(args))
        if self.args.command:
            self.command = self.args.command
        if self.args.list:
            for image in self.args.list:
                # Make fully-qualified image name
                colons = image.count(':')
                if colons == 0:
                    image = image + ":latest"
                slashes = image.count('/')
                if slashes == 0:
                    image = "library/" + image
            self.images.append(image)
        # Cheap way to deduplicate lists
        self.images = list(set(self.images))
        if self.images:
            self.images.sort()

    def update_images_from_repo(self):
        """Scan the repo looking for images.
        """
        if not self.repo:
            self.repo = ScanRepo(host=self.args.repo,
                                 path=self.args.path,
                                 owner=self.args.owner,
                                 name=self.args.name,
                                 dailies=self.args.dailies,
                                 weeklies=self.args.weeklies,
                                 releases=self.args.releases,
                                 json=True, insecure=self.args.insecure,
                                 sort_field=self.args.sort,
                                 debug=self.args.debug)
        if not self.args.no_scan:
            if self.args.repo:
                self.logger.debug("Scanning '%s' for images" % self.args.repo)
            else:
                self.logger.debug("Scanning Docker repo for images")
            self.repo.scan()
            self.logger.debug("Scan Data: %s" % json.dumps(self.repo.data,
                                                           sort_keys=True,
                                                           indent=4))
            scan_imgs = []
            for section in ["daily", "weekly", "release"]:
                for entry in self.repo.data[section]:
                    exhost = ''
                    if self.args.repo:
                        exhost = self.args.repo
                        if self.args.port:
                            exhost += ":" + self.args.port + "/"
                    scan_imgs.append(exhost + self.args.owner + "/" +
                                     self.args.name + ":" +
                                     entry["name"])
            current_imgs = [x for x in self.images]
            # Dedupe by running the list through a set.
            current_imgs.extend(scan_imgs)
            current_imgs = list(set(current_imgs))
            if current_imgs:
                current_imgs.sort()
            self.images = current_imgs

    def build_nodelist(self):
        """Make a list of all schedulable nodes.
        """
        v1 = self.client
        logger = self.logger
        logger.debug("Getting schedulable node list.")
        v1nodelist = v1.list_node()
        nodes = []
        for thing in v1nodelist.items:
            spec = thing.spec
            if spec.unschedulable:
                continue
            if spec.taints:
                taints = [x.effect for x in spec.taints]
                if "NoSchedule" in taints:
                    continue
            nodes.append(thing.metadata.name)
        logger.debug("Schedulable list: %s" % str(nodes))
        self.nodes = nodes

    def build_pod_specs(self):
        """Build a set of Pod specs from our image list and our node list.
        """
        specs = []
        for img in self.images:
            for node in self.nodes:
                specs.append(self._build_pod_spec(img, node))
        self.pod_specs = specs
        self.logger.debug("Specs: %s" % str(self.pod_specs))

    def _build_pod_spec(self, img, node):
        spec = client.V1PodSpec(
            containers=[
                client.V1Container(
                    command=self.command,
                    image=img,
                    image_pull_policy="Always",
                    name=self._extract_podname(img)
                )
            ],
            restart_policy="Never",
            node_name=node
        )
        return spec

    def _extract_podname(self, img):
        iname = '-'.join(img.split('/')[-2:])
        iname = iname.replace(':', '-')
        return iname

    def run_pods(self):
        """Run a pod, with a single container, on a particular node.
        This has the effect of pulling the image for that pod onto that
        node.  The run itself is unimportant.
        """
        v1 = self.client
        made_pods = []
        for pod_spec in self.pod_specs:
            img = pod_spec.containers[0].image
            imgname = self._extract_podname(img)
            name = "pp-" + imgname + "-" + pod_spec.node_name.split('-')[-1]
            pod = client.V1Pod(spec=pod_spec,
                               metadata=client.V1ObjectMeta(
                                   name=name)
                               )
            self.logger.debug("Running pod %s" % str(pod_spec))
            made_pod = v1.create_namespaced_pod(self.namespace, pod)
            podname = made_pod.metadata.name
            made_pods.append(podname)
        self.created_pods = made_pods

    def wait_for_pods(self):
        """Uses created_pods and loops until each of those is in phase
        "Succeeded" or "Failed".
        """
        created_pods = self.created_pods
        v1 = self.client
        failuremap = {}
        while True:
            pods = v1.list_namespaced_pod(self.namespace).items
            podmap = {}
            for cpod in created_pods:
                podmap[cpod] = "unknown"
            for pod in pods:
                pname = pod.metadata.name
                if pname not in created_pods:
                    continue
                podmap[pname] = pod.status.phase
            any_still_going = False
            for cpod in created_pods:
                if podmap[cpod] not in ["Failed", "Succeeded"]:
                    any_still_going = True
                    self.logger.debug("Pod %s in status %s" %
                                      (cpod, podmap[cpod]))
                    break
                if podmap[cpod] == "Failed":
                    if cpod not in failuremap:
                        failuremap[cpod] = True
                        self.logger.error("Pod %s failed")
            if any_still_going:
                self.logger.debug("Need to keep waiting for pods.")
                time.sleep(1)
                continue
            break

    def delete_pods(self):
        v1 = self.client
        for pod in self.created_pods:
            self.logger.debug("Deleting pod %s" % pod)
            v1.delete_namespaced_pod(
                pod, self.namespace, client.V1DeleteOptions())
