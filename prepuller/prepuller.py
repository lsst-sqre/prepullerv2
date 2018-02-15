import argparse
import json
import logging
import sys
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from .scanrepo import ScanRepo


class Prepuller(object):
    """Class for generating and reaping the DaemonSets for the prepuller.
    """
    repo = None
    logger = None
    client = None
    args = argparse.Namespace(debug=False,
                              json=True,
                              repo="hub.docker.com",
                              owner="lsstsqre",
                              name="jld-lab",
                              port=None,
                              dailies=3,
                              weeklies=2,
                              releases=1,
                              insecure=False,
                              sort="comp_ts",
                              list=None,
                              command=("/opt/lsst/software/" +
                                       "jupyterlab/prepuller.sh"),
                              path="/v2/repositories/lsstsqre/jld-lab/tags/",
                              no_scan=False)
    images = []
    daemonset_specs = []

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
        try:
            config.load_incluster_config()
        except ConfigException:
            try:
                config.load_kube_config()
            except Exception:
                logging.critical(sys.argv[0], " must be run from a system",
                                 " with k8s API access.")
                raise
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
                if slashes < 2:
                    image = "registry-1.docker.io/" + image
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
            self.logger.debug("Scanning '%s' for images" % self.args.repo)
            self.repo.scan()
            self.logger.debug("Scan Data: %s" % json.dumps(self.repo.data,
                                                           sort_keys=True,
                                                           indent=4))
            scan_imgs = []
            for section in ["daily", "weekly", "release"]:
                for entry in self.repo.data[section]:
                    exhost = self.args.repo
                    if self.args.port:
                        exhost += ":" + self.args.port
                    scan_imgs.append('/'.join([exhost, self.args.owner,
                                               self.args.name]) + ":" +
                                     entry["name"])
            current_imgs = [x for x in self.images]
            # Dedupe by running the list through a set.
            current_imgs.extend(scan_imgs)
            current_imgs = list(set(current_imgs))
            if current_imgs:
                current_imgs.sort()
            self.images = current_imgs

    def build_daemonset_specs(self):
        """Build a set of DaemonSet specs from our image list.
        """
        specs = []
        for img in self.images:
            specs.append(self._build_daemonset_spec(img))
        self.daemonset_specs = specs

    def _build_daemonset_spec(self, img):
        tag = img.split(':')[-1]
        imgname = "pp-%s" % tag
        spec = {"kind": "DaemonSet",
                "metadata": {
                    "name": imgname
                },
                "spec": {
                    "template": {
                        "metadata": {
                            "labels": {
                                "app": imgname
                            }
                        },
                        "spec": {
                            "containers": [
                                {"name": imgname,
                                 "image": img,
                                 "command": [self.command]
                                 }
                            ]
                        }
                    }
                }
                }
        return spec

    def execute_daemonsets(self):
        for spec in self.daemonset_specs:
            self._run_daemonset(spec)
            self._wait_for_daemonset(spec)
            self._destroy_daemonset(spec)

    def _run_daemonset(self, spec):
        pass

    def _wait_for_daemonset(self, spec):
        pass

    def _destroy_daemonset(self, spec):
        pass
