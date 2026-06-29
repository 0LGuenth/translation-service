resource "openstack_compute_instance_v2" "master" {
  name            = "${var.vm_prefix}-master"
  image_id        = var.image_id
  flavor_name     = var.flavor_name
  key_pair        = var.key_pair
  security_groups = ["default"]
  network { name = var.network_name }
  timeouts { create = "10m" }
}

resource "openstack_compute_instance_v2" "worker" {
  count           = var.worker_count
  name            = "${var.vm_prefix}-worker-${count.index + 1}"
  image_id        = var.image_id
  flavor_name     = var.flavor_name
  key_pair        = var.key_pair
  security_groups = ["default"]
  network { name = var.network_name }
  timeouts { create = "10m" }
}
