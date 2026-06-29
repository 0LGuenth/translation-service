variable "auth_url" {
  description = "OpenStack Identity API endpoint"
  default     = "https://stack.dhbw.cloud:5000"
}

variable "app_cred_id" {
  description = "OpenStack application credential ID."
  type        = string
  sensitive   = true
}

variable "app_cred_secret" {
  description = "OpenStack application credential secret."
  type        = string
  sensitive   = true
}

variable "image_id" {
  description = "OpenStack image ID"
  default     = "7c6e4b52-4d1f-4da9-b98e-23b17e6570a5"
}

variable "flavor_name" {
  description = "OpenStack flavor name."
  default     = "m1.extra_large"
}

variable "key_pair" {
  description = "Name of the SSH keypair already uploaded to DHBWCloud — no default, set in terraform.tfvars."
  type        = string
}

variable "network_name" {
  description = "OpenStack network the VMs attach to."
  default     = "DHBW"
}

variable "vm_prefix" {
  description = "Prefix for all VM names (e.g. \"<lastname>-translate\")."
  type        = string
}

variable "worker_count" {
  description = "Number of worker nodes. Lecture uses 2 (plus 1 control-plane)."
  default     = 2
}
