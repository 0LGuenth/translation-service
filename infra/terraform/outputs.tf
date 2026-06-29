locals {
  all_nodes = concat([openstack_compute_instance_v2.master], openstack_compute_instance_v2.worker)

  cluster_has_v6 = alltrue([for n in local.all_nodes : length(n.network[0].fixed_ip_v6) > 0])
  cluster_has_v4 = alltrue([for n in local.all_nodes : length(n.network[0].fixed_ip_v4) > 0])

  ip_family         = local.cluster_has_v6 ? (local.cluster_has_v4 ? "dual" : "ipv6") : "ipv4"
  ip_family_primary = "ipv6" # only consulted for "dual"; IPv4 is NAT-only, so IPv6 leads.

  # Connection address per node: IPv6 when available, the NAT IPv4 otherwise.
  conn_addr = { for n in local.all_nodes :
    n.name => local.cluster_has_v6 ? n.network[0].fixed_ip_v6 : n.network[0].fixed_ip_v4
  }
  master_addr = local.conn_addr[openstack_compute_instance_v2.master.name]
}

output "master_ip" {
  value = local.master_addr
}

output "worker_ips" {
  value = [for w in openstack_compute_instance_v2.worker : local.conn_addr[w.name]]
}

output "ip_family" {
  value = local.ip_family
}

# Ansible inventory
resource "local_file" "ansible_inventory" {
  filename = "${path.module}/generated-inventory.yml"
  content = yamlencode({
    all = {
      children = {
        translate = {
          # Inherited by both server and agents so the whole cluster shares one stack.
          vars = {
            interpreter_python = "/usr/bin/python3"
            ip_family          = local.ip_family
            ip_family_primary  = local.ip_family_primary
          }
          children = {
            translate_k3s_server = {
              vars = { k3s_role = "server" }
              hosts = {
                (local.master_addr) = {
                  ansible_user = "ubuntu"
                }
              }
            }
            translate_k3s_agent = {
              vars = {
                k3s_role        = "agent"
                k3s_server_host = local.master_addr
              }
              hosts = {
                for w in openstack_compute_instance_v2.worker :
                local.conn_addr[w.name] => {
                  ansible_user = "ubuntu"
                }
              }
            }
          }
        }
      }
    }
  })
}
