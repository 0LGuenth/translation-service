terraform {
  required_providers {
    local = {
      source = "hashicorp/local"
    }
    openstack = {
      source = "terraform-provider-openstack/openstack"
    }
  }
}

# Application credentials live in terraform.tfvars.
provider "openstack" {
  domain_name                   = "default"
  auth_url                      = var.auth_url
  application_credential_id     = var.app_cred_id
  application_credential_secret = var.app_cred_secret
}
