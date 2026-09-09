terraform {
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "2.9.0"
    }
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "3.4.0"
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
