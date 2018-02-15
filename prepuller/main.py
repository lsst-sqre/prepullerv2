#!/usr/bin/env python3
import argparse
from .prepuller import Prepuller


def standalone():
    args = parse_args()
    prepuller = Prepuller(args=args)
    prepuller.update_images_from_repo()
    prepuller.build_daemonset_specs()
    print(prepuller.daemonset_specs)


def parse_args():
    """Parse command-line arguments"""
    desc = "Set up DaemonSets to prepull."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("-d", "--debug", action="store_true",
                        help="enable debugging")
    parser.add_argument("-r", "--repo", "--repository",
                        help="repository host [hub.docker.com]",
                        default="hub.docker.com")
    parser.add_argument("-o", "--owner", "--organization", "--org",
                        help="repository owner [lsstsqre]",
                        default="lsstsqre")
    parser.add_argument("-n", "--name",
                        help="repository name [jld-lab]",
                        default="jld-lab")
    parser.add_argument("-q", "--dailies", "--daily", "--quotidian", type=int,
                        help="# of daily builds to keep [3]",
                        default=3)
    parser.add_argument("-w", "--weeklies", "--weekly", type=int,
                        help="# of weekly builds to keep [2]",
                        default=2)
    parser.add_argument("-b", "--releases", "--release", type=int,
                        help="# of release builds to keep [1]",
                        default=1)
    parser.add_argument("-i", "--insecure", "--no-tls", "--no-ssl",
                        help="Do not use TLS to connect [False]",
                        type=bool,
                        default=False)
    parser.add_argument("-l", "--list", "--list-images", "--image-list",
                        help=("Use supplied comma-separated list in" +
                              " addition to repo scan"))
    parser.add_argument("-p", "--port", help="Repository port [443 for" +
                        " secure, 80 for insecure]",
                        default=None)
    parser.add_argument("-s", "--sort", "--sort-field", "--sort-by",
                        help="Field to sort results by [comp_ts]",
                        default="comp_ts")
    parser.add_argument("--no-scan", action="store_true",
                        help="Do not do repo scan (only useful in" +
                        " conjunction with --list)")
    parser.add_argument("-c", "--command", help="Command to run when image" +
                        " is run as prepuller",
                        default="/opt/lsst/software/jupyterlab/prepuller.sh")
    results = parser.parse_args()
    results.path = ("/v2/repositories/" + results.owner + "/" +
                    results.name + "/tags/")
    if results.list:
        results.list = list(set(results.list.split(',')))
    return results

if __name__ == "__main__":
    standalone()
