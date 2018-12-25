# DM Ansible 部署方案

## 概述

Ansible 是一款自动化运维工具，DM-Ansible 是 PingCAP 基于 Ansible Playbook 功能编写的集群部署工具。使用 DM-Ansible 可以快速部署一个完整的 DM 集群。

本部署工具可以通过配置文件设置集群拓扑，完成以下各项运维工作：

- 初始化部署机器操作系统参数
- 部署 DM 集群组件（包括 dm-master、dm-worker、dmctl）
- 部署监控组件（包括 prometheus、grafana、alertmanager）
- 启动集群
- 关闭集群
- 升级组件版本

## 准备机器

1.  部署目标机器若干

    - 推荐安装 CentOS 7.3 及以上版本 Linux 操作系统，x86_64 架构 (amd64)。
    - 机器之间内网互通。
    - 关闭防火墙或开放[服务端口](#服务默认端口)。

2.  部署中控机一台:

    - 中控机可以是部署目标机器中的某一台。
    - 推荐安装 CentOS 7.3 及以上版本 Linux 操作系统（默认包含 Python 2.7）。
    - 该机器需开放外网访问，用于下载 DM 及相关软件安装包。

## 在中控机上安装系统依赖包

以 `root` 用户登录中控机

如果中控机是 CentOS 7 系统，执行以下命令：

```
# yum -y install epel-release git curl sshpass
# yum -y install python-pip
```

如果是中控机是 Ubuntu 系统，执行以下命令：

```
# apt-get -y install git curl sshpass python-pip
```

## 在中控机上创建 tidb 用户，并生成 ssh key

以 `root` 用户登录中控机，执行以下命令

创建 `tidb` 用户

```
# useradd -m -d /home/tidb tidb
```

设置 `tidb` 用户密码

```
# passwd tidb
```

配置 tidb 用户 sudo 免密码，将 tidb ALL=(ALL) NOPASSWD: ALL 添加到文件末尾即可。

```
# visudo
tidb ALL=(ALL) NOPASSWD: ALL
```

生成 ssh key：执行 `su` 命令从 `root` 用户切换到 `tidb` 用户下，创建 `tidb` 用户 ssh key， 提示 `Enter passphrase` 时直接回车即可。

执行成功后，ssh 私钥文件为 `/home/tidb/.ssh/id_rsa`， ssh 公钥文件为 `/home/tidb/.ssh/id_rsa.pub`。

```
# su - tidb
```

```
$ ssh-keygen -t rsa
Generating public/private rsa key pair.
Enter file in which to save the key (/home/tidb/.ssh/id_rsa):
Created directory '/home/tidb/.ssh'.
Enter passphrase (empty for no passphrase):
Enter same passphrase again:
Your identification has been saved in /home/tidb/.ssh/id_rsa.
Your public key has been saved in /home/tidb/.ssh/id_rsa.pub.
The key fingerprint is:
SHA256:eIBykszR1KyECA/h0d7PRKz4fhAeli7IrVphhte7/So tidb@172.16.10.49
The key's randomart image is:
+---[RSA 2048]----+
|=+o+.o.          |
|o=o+o.oo         |
| .O.=.=          |
| . B.B +         |
|o B * B S        |
| * + * +         |
|  o + .          |
| o  E+ .         |
|o   ..+o.        |
+----[SHA256]-----+
```

## 在中控机器上下载 DM-Ansible

以 `tidb` 用户登录中控机并进入 `/home/tidb` 目录。

```
$ wget http://download.pingcap.org/dm-ansible.tar.gz
```

## 在中控机器上安装 Ansible 及其依赖

以 `tidb` 用户登录中控机，请务必按以下方式通过 pip 安装 Ansible 及其相关依赖的指定版本，否则会有兼容问题。安装完成后，可通过 `ansible --version` 查看 Ansible 版本。目前 DM-Ansible 兼容 Ansible 2.5 版本及以上版本，Ansible 及相关依赖版本记录在 `dm-ansible/requirements.txt` 文件中。

  ```bash
  $ tar -xzvf dm-ansible.tar.gz
  $ cd /home/tidb/dm-ansible
  $ sudo pip install -r ./requirements.txt
  $ ansible --version
    ansible 2.5.0
  ```

## 在中控机上配置部署机器 ssh 互信及 sudo 规则

以 `tidb` 用户登录中控机，将你的部署目标机器 IP 添加到 `hosts.ini` 文件 `[servers]` 区块下。

```
$ cd /home/tidb/dm-ansible
$ vi hosts.ini
[servers]
172.16.10.71
172.16.10.72
172.16.10.73

[all:vars]
username = tidb
```

执行以下命令，按提示输入部署目标机器 `root` 用户密码。该步骤将在部署目标机器上创建 `tidb` 用户，并配置 sudo 规则，配置中控机与部署目标机器之间的 ssh 互信。

```
$ ansible-playbook -i hosts.ini create_users.yml -u root -k
```

## 联网下载 DM 及监控组件安装包到中控机

执行 `local_prepare.yml` playbook，联网下载 DM 及监控组件安装包到中控机：

```
ansible-playbook local_prepare.yml
```

## 编辑 inventory.ini 文件, 配置集群拓扑

以 `tidb` 用户登录中控机，`inventory.ini` 文件路径为 `/home/tidb/dm-ansible/inventory.ini`。

> **注：** 请使用内网 IP 来部署集群，如果部署目标机器 SSH 端口非默认 22 端口，需添加 `ansible_port` 变量，如：
> `dm-worker1 ansible_host=172.16.10.72 ansible_port=5555 server_id=101 mysql_host=172.16.10.72 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306`

### 样例集群拓扑

| Name | Host IP | Services |
| ---- | ------- | -------- |
| node1 | 172.16.10.71 | dm-master, prometheus, grafana, alertmanager |
| node2 | 172.16.10.72 | dm-worker |
| node3 | 172.16.10.73 | dm-worker |

```ini
## DM modules
[dm_master_servers]
dm_master ansible_host=172.16.10.71

[dm_worker_servers]
dm_worker1 ansible_host=172.16.10.72 server_id=101 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

dm_worker2 ansible_host=172.16.10.73 server_id=102 mysql_host=172.16.10.82 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

## Monitoring modules
[prometheus_servers]
prometheus ansible_host=172.16.10.71

[grafana_servers]
grafana ansible_host=172.16.10.71

[alertmanager_servers]
alertmanager ansible_host=172.16.10.71

## Global variables
[all:vars]
cluster_name = test-cluster

ansible_user = tidb

dm_version = latest

deploy_dir = /data1/dm

grafana_admin_user = "admin"
grafana_admin_password = "admin"
```

### inventory.ini 变量调整

#### 部署目录调整

部署目录通过 `deploy_dir` 变量控制，默认全局变量已设置为 `/home/tidb/deploy`，对所有服务生效。如数据盘挂载目录为 `/data1`，可设置为 `/data1/dm`，样例如下:

```
## Global variables
[all:vars]
deploy_dir = /data1/dm
```

如为某一服务单独设置部署目录，可在配置服务主机列表时配置主机变量，以 dm-master 节点为例，其他服务类推，请务必添加第一列别名，以免服务混布时混淆。

```
dm-master ansible_host=172.16.10.71 deploy_dir=/data1/deploy
```

#### 全局变量解释

| 变量            | 含义                                                        |
| --------------- | ---------------------------------------------------------- |
| cluster_name | 集群名称，可调整 |
| dm_version | DM 版本，默认已配置 |
| grafana_admin_user | Grafana 管理员帐号用户名，默认为 admin |
| grafana_admin_password | Grafana 管理员帐号密码，默认为 admin，用于 Ansible 导入 Dashboard，如后期通过 grafana web 修改了密码，请更新此变量 |

#### dm-worker 配置参数解释

| 变量 | 变量含义 |
| ------------- | ------- |
 | source_id | dm-worker 绑定到唯一的一个数据库实例/具有主从架构的复制组，当发生主从切换的时候，只需要更新 mysql_host/port 而不用更改该 ID 标识|
| server_id | dm-worker 伪装成一个 mysql slave，即 slave 的 server_id, 需要在 mysql 集群中全局唯一，取值范围 0 - 4294967295 |
| mysql_host | 上游 MySQL host | 
| mysql_user | 上游 MySQL 用户名，默认为 root |
| mysql_password | 上游 MySQL 用户名密码，密码需使用 dmctl 工具加密，参考 [dmctl 加密上游 MySQL 密码](#dmctl-加密上游-mysql-用户密码) |
| mysql_port | 上游 MySQL 端口号, 默认为 3306 |
| enable_gtid | dm-worker 是否要用 GTID 形式的位置去拉取 binlog，前提是上游 mysql 已经开启 GTID 模式 |
| relay_binlog_name | dm-worker 是否要从该指定 binlog file 开始拉取 binlog，仅本地不存在有效 relay log 时使用 |
| relay_binlog_gtid | dm-worker 是否要从该指定 GTID 开始拉取 binlog，仅本地不存在有效 relay log 且 enable_gtid 为 true 时使用 |
| flavor | flavor 表示 mysql 的发行版类型，官方版以及 percona、云 mysql 填写 mysql，mariadb 则填写 mariadb，默认为 mysql |

#### dmctl 加密上游 MySQL 用户密码

以上游 MySQL 用户密码为 `123456` 为例，将生成的字符串配置到 dm-worker 的 `mysql_password` 变量中。

```
$ cd /home/tidb/dm-ansible/resources/bin
$ ./dmctl -encrypt 123456
VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=
```

## 部署任务

> ansible-playbook 执行 Playbook 时默认并发为 5，部署目标机器较多时可添加 -f 参数指定并发，如 `ansible-playbook deploy.yml -f 10` 

1. 确认 `dm-ansible/inventory.ini` 文件中 `ansible_user = tidb`，本例使用 `tidb` 用户作为服务运行用户，配置如下：

    > `ansible_user` 不要设置成 `root` 用户，`dm-ansbile` 限制了服务以普通用户运行。

    执行以下命令如果所有 server 返回 `tidb` 表示 ssh 互信配置成功。

    ```
    ansible -i inventory.ini all -m shell -a 'whoami'
    ```

    执行以下命令如果所有 server 返回 `root` 表示 `tidb` 用户 sudo 免密码配置成功。

    ```
    ansible -i inventory.ini all -m shell -a 'whoami' -b
    ```

2.  修改内核参数，并部署 DM 集群组件及监控组件

    ```
    ansible-playbook deploy.yml
    ```

3.  启动 DM 集群

    ```
    ansible-playbook start.yml
    ```

## 常见运维操作

### 启动集群

此操作会按顺序启动整个 DM 集群所有组件（包括 dm-master、dm-worker 和监控组件）。

```
$ ansible-playbook start.yml
```

### 关闭集群

此操作会按顺序关闭整个 DM 集群所有组件（包括 dm-master、dm-worker 和监控组件）。

```
$ ansible-playbook stop.yml
```

### 升级组件版本

#### 下载 DM binary

1. 删除原有的 downloads 目录下文件。

```
$ cd /home/tidb/dm-ansible
$ rm -rf downloads
```

2. 使用 playbook 下载最新的 DM binary，自动替换 `/home/tidb/dm-ansible/resource/bin/` 目录下 binary 文件。

```
$ ansible-playbook local_prepare.yml
```

#### 使用 Ansible 滚动升级

- 滚动升级 dm-worker 实例

```
ansible-playbook rolling_update.yml --tags=dm-worker
```

- 滚动升级 dm-master 实例

```
ansible-playbook rolling_update.yml --tags=dm-master
```

- 升级 dmctl

```
ansible-playbook rolling_update.yml --tags=dmctl
```

- 滚动升级 dm-worker、dm-master 和 dmctl

```
ansible-playbook rolling_update.yml
```

### 增加 dm-worker 实例

假设在 172.16.10.74 上新增 dm-worker 实例，实例别名为 `dm_worker3`。

#### 在中控机上配置部署机器 ssh 互信及 sudo 规则

参考[在中控机上配置部署机器 ssh 互信及 sudo 规则](#在中控机上配置部署机器-ssh-互信及-sudo-规则)。以 `tidb` 用户登录中控机，将 `172.16.10.74` 添加到 `hosts.ini` 文件 `[servers]` 区块下。

```
$ cd /home/tidb/dm-ansible
$ vi hosts.ini
[servers]
172.16.10.74

[all:vars]
username = tidb
```

执行以下命令，按提示输入部署 `172.16.10.74` `root` 用户密码。该步骤将在 `172.16.10.74` 上创建 `tidb` 用户，并配置 sudo 规则，配置中控机与 `172.16.10.74` 之间的 ssh 互信。

```
$ ansible-playbook -i hosts.ini create_users.yml -u root -k
```

#### 编辑 inventory.ini 文件, 添加新增 dm-worker 实例

编辑 `inventory.ini` 文件，根据实际信息，添加新增 dm_worker 实例 dm_worker3。

```
[dm_worker_servers]
dm_worker1 ansible_host=172.16.10.72 server_id=101 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

dm_worker2 ansible_host=172.16.10.73 server_id=102 mysql_host=172.16.10.82 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

dm_worker3 ansible_host=172.16.10.74 server_id=103 mysql_host=172.16.10.83 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
```

#### 部署新增 dm-worker 实例

```
$ ansible-playbook deploy.yml --tags=dm-worker -l dm_worker3
```

#### 启动新增 dm-worker 实例

```
$ ansible-playbook start.yml --tags=dm-worker -l dm_worker3
```

#### 配置并重启 dm-master 服务

```
$ ansible-playbook rolling_update.yml --tags=dm-master
```

#### 配置并重启 prometheus 服务

```
$ ansible-playbook rolling_update_monitor.yml --tags=prometheus
```

### 下线 dm-worker 实例

假设下线 `dm_worker3` 实例。

#### 关闭下线 dm-worker 实例

```
$ ansible-playbook stop.yml --tags=dm-worker -l dm_worker3
```

#### 编辑 inventory.ini 文件，移除下线 dm-worker 实例信息

编辑 `inventory.ini` 文件，注释或删除 dm_worker3 实例所在行。

```
[dm_worker_servers]
dm_worker1 ansible_host=172.16.10.72 server_id=101 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

dm_worker2 ansible_host=172.16.10.73 server_id=102 mysql_host=172.16.10.82 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

# dm_worker3 ansible_host=172.16.10.74 server_id=103 mysql_host=172.16.10.83 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306 # 注释或删除该行
```

#### 配置并重启 dm-master 服务

```
$ ansible-playbook rolling_update.yml --tags=dm-master
```

#### 配置并重启 prometheus 服务

```
$ ansible-playbook rolling_update_monitor.yml --tags=prometheus
```

### 替换 dm-master 实例

假设 `172.16.10.71` 机器需要维护或故障，需要将 dm-master 实例从 `172.16.10.71` 机器迁移到 `172.16.10.80` 机器上。

#### 在中控机上配置部署机器 ssh 互信及 sudo 规则

参考[在中控机上配置部署机器 ssh 互信及 sudo 规则](#在中控机上配置部署机器-ssh-互信及-sudo-规则)。以 `tidb` 用户登录中控机，将 `172.16.10.80` 添加到 `hosts.ini` 文件 `[servers]` 区块下。

```
$ cd /home/tidb/dm-ansible
$ vi hosts.ini
[servers]
172.16.10.80

[all:vars]
username = tidb
```

执行以下命令，按提示输入部署 `172.16.10.80` `root` 用户密码。该步骤将在 `172.16.10.80` 上创建 `tidb` 用户，并配置 sudo 规则，配置中控机与 `172.16.10.80` 之间的 ssh 互信。

```
$ ansible-playbook -i hosts.ini create_users.yml -u root -k
```

#### 关闭旧 dm-master 实例

> 如果 `172.16.10.71` 机器故障，无法 SSH 登录，此步骤请忽略。

```
$ ansible-playbook stop.yml --tags=dm-master
```

#### 编辑 inventory.ini 文件，配置 dm-master 实例信息

编辑 `inventory.ini` 文件，注释或删除旧 dm-master 实例所在行，增加新 dm-master 实例信息。

```
[dm_master_servers]
# dm_master ansible_host=172.16.10.71
dm_master ansible_host=172.16.10.80
```

#### 部署新 dm-master 实例

```
$ ansible-playbook deploy.yml --tags=dm-master
```

#### 启动新 dm-master 实例

```
$ ansible-playbook start.yml --tags=dm-master
```

#### 更新 dmctl 配置文件

```
ansible-playbook rolling_update.yml --tags=dmctl
```

### 替换 dm-worker 实例

假设 `172.16.10.72` 机器需要维护或故障，需要将 dm-worker 实例 `dm_worker1` 从 `172.16.10.72` 机器迁移到 `172.16.10.75` 机器上。

#### 在中控机上配置部署机器 ssh 互信及 sudo 规则

参考[在中控机上配置部署机器 ssh 互信及 sudo 规则](#在中控机上配置部署机器-ssh-互信及-sudo-规则)。以 `tidb` 用户登录中控机，将 `172.16.10.75` 添加到 `hosts.ini` 文件 `[servers]` 区块下。

```
$ cd /home/tidb/dm-ansible
$ vi hosts.ini
[servers]
172.16.10.75

[all:vars]
username = tidb
```

执行以下命令，按提示输入部署 `172.16.10.75` `root` 用户密码。该步骤将在 `172.16.10.75` 上创建 `tidb` 用户，并配置 sudo 规则，配置中控机与 `172.16.10.75` 之间的 ssh 互信。

```
$ ansible-playbook -i hosts.ini create_users.yml -u root -k
```

#### 关闭旧 dm-worker 实例

> 如果 `172.16.10.72` 机器故障，无法 SSH 登录，此步骤请忽略。

```
$ ansible-playbook stop.yml --tags=dm-worker -l dm_worker1
```

#### 编辑 inventory.ini 文件, 添加新 dm-worker 实例

编辑 `inventory.ini` 文件，根据实际信息，注释或删除旧 dm_worker1 实例 `172.16.10.72` 所在行，增加新 dm_worker1 实例 `172.16.10.75` 信息。

```
[dm_worker_servers]
dm_worker1 ansible_host=172.16.10.75 server_id=101 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
# dm_worker1 ansible_host=172.16.10.72 server_id=101 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

dm_worker2 ansible_host=172.16.10.73 server_id=102 mysql_host=172.16.10.82 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
```

#### 部署新 dm-worker 实例

```
$ ansible-playbook deploy.yml --tags=dm-worker -l dm_worker1
```

#### 启动新 dm-worker 实例

```
$ ansible-playbook start.yml --tags=dm-worker -l dm_worker1
```

#### 配置并重启 dm-master 服务

```
$ ansible-playbook rolling_update.yml --tags=dm-master
```

#### 配置并重启 prometheus 服务

```
$ ansible-playbook rolling_update_monitor.yml --tags=prometheus
```

## 常见部署问题

### 服务默认端口

| 组件 | 端口变量 | 默认端口 | 说明 |
| :-- | :-- | :-- | :-- |
| dm-master | dm_master_port | 8261  | dm-master 服务通信端口 |
| dm-worker | dm_worker_port | 8262  | dm-worker 服务通信端口 |
| Prometheus | prometheus_port | 9090 | Prometheus 服务通信端口 |
| Grafana | grafana_port |  3000 | Web 监控服务对外服务和客户端(浏览器)访问端口 |
| Alertmanager | alertmanager_port |  9093 | Alertmanager 服务通信端口 |

### 如何自定义端口

修改 `inventory.ini` 文件，在相应服务 IP 后添加对应服务端口相关主机变量即可：

```
dm_master ansible_host=172.16.10.71 dm_master_port=18261
```

### 如何更新 dm-ansible

以 tidb 用户登录中控机并进入 /home/tidb 目录，备份 dm-ansible 文件夹。

```
$ cd /home/tidb
$ mv dm-ansible dm-ansible-bak
```

下载最新的 dm-ansible 并解压

```
$ cd /home/tidb
$ wget http://download.pingcap.org/dm-ansible.tar.gz
$ tar -xzvf dm-ansible.tar.gz
```

迁移 `inventory.ini` 配置文件

```
$ cd /home/tidb
$ cp dm-ansible-bak/inventory.ini dm-ansible/inventory.ini
```

迁移 dmctl 配置

```
$ cd /home/tidb/dm-ansible-bak/dmctl
$ cp * /home/tidb/dm-ansible/dmctl/
```

使用 playbook 下载最新的 DM binary，自动替换 `/home/tidb/dm-ansible/resource/bin/` 目录下 binary 文件。

```
$ ansible-playbook local_prepare.yml
```

### 单机部署多个 dm-worker 样例

以下为单机部署多个 dm-worker `inventory.ini` 文件样例，配置时请注意区分 `server_id`， `deploy_dir`，`dm_worker_port` 变量。

```ini
## DM modules
[dm_master_servers]
dm_master ansible_host=172.16.10.71

[dm_worker_servers]
dm_worker1_1 ansible_host=172.16.10.72 server_id=101 deploy_dir=/data1/dm_worker dm_worker_port=8262 mysql_host=172.16.10.81 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
dm_worker1_2 ansible_host=172.16.10.72 server_id=102 deploy_dir=/data2/dm_worker dm_worker_port=8263 mysql_host=172.16.10.82 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

dm_worker2_1 ansible_host=172.16.10.73 server_id=103 deploy_dir=/data1/dm_worker dm_worker_port=8262 mysql_host=172.16.10.83 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306
dm_worker2_2 ansible_host=172.16.10.73 server_id=104 deploy_dir=/data2/dm_worker dm_worker_port=8263 mysql_host=172.16.10.84 mysql_user=root mysql_password='VjX8cEeTX+qcvZ3bPaO4h0C80pe/1aU=' mysql_port=3306

## Monitoring modules
[prometheus_servers]
prometheus ansible_host=172.16.10.71

[grafana_servers]
grafana ansible_host=172.16.10.71

[alertmanager_servers]
alertmanager ansible_host=172.16.10.71

## Global variables
[all:vars]
cluster_name = test-cluster

ansible_user = tidb

dm_version = latest

deploy_dir = /data1/dm

grafana_admin_user = "admin"
grafana_admin_password = "admin"
```
