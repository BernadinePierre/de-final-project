terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "6.4.0"
        }
    }

    backend "s3" {
        bucket = "tf-manager-26-08-2025"
        key = "terraform.tfstate"
        region = "eu-west-2"
    }
}

provider "aws" {
    region = "eu-west-2"
}