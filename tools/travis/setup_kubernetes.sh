# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
if [[ `uname -i` == 'aarch64' ]]; then
  # minikube doesn't support ARM platform. Use Kubeadm instead.
  # 1. Install kubelet kubeadm kubectl
  sudo apt-get update && apt-get install -y apt-transport-https curl
  curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
  sudo echo deb https://apt.kubernetes.io/ kubernetes-xenial main >> /etc/apt/sources.list.d/kubernetes.list
  sudo apt-get update
  sudo apt-get install -y kubelet kubeadm kubectl
  sudo apt-mark hold kubelet kubeadm kubectl
  # 2. Configure
  sudo modprobe br_netfilter
  sudo sysctl net.bridge.bridge-nf-call-iptables=1
  sudo echo 1 > /proc/sys/net/ipv4/ip_forward
  # 3. Install kubernetes
  kubeadm init --kubernetes-version v1.16.0 --pod-network-cidr=10.244.0.0/16
  # 4. Config command line and network
  mkdir -p $HOME/.kube
  sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
  sudo chown $(id -u):$(id -g) $HOME/.kube/config
  kubectl apply -f https://raw.githubusercontent.com/coreos/flannel/2140ac876ef134e0ed5af15c65e414cf26827915/Documentation/kube-flannel.yml
  # 5. Make the master node schedulerable
  kubectl taint nodes --all node-role.kubernetes.io/master-
else
  # Download kubectl, which is a requirement for using minikube.
  curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl && chmod +x kubectl && sudo mv kubectl /usr/local/bin/
  # Download minikube.
  curl -Lo minikube https://storage.googleapis.com/minikube/releases/v0.25.2/minikube-linux-amd64 && chmod +x minikube && sudo mv minikube /usr/local/bin/
  sudo minikube start --vm-driver=none --kubernetes-version=v1.9.0
  # Fix the kubectl context, as it's often stale.
  minikube update-context
  # Wait for Kubernetes to be up and ready.
  JSONPATH='{range .items[*]}{@.metadata.name}:{range @.status.conditions[*]}{@.type}={@.status};{end}{end}'; until kubectl get nodes -o jsonpath="$JSONPATH" 2>&1 | grep -q "Ready=True"; do sleep 1; done
fi
