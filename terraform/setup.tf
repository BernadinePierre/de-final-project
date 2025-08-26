resource "aws_s3_bucket" "ingest_bucket" {
    bucket = "nc-crigglestone-ingest-bucket"
    object_lock_enabled = true
}

resource "aws_s3_bucket" "processed_bucket" {
    bucket = "nc-crigglestone-processed-bucket"
    object_lock_enabled = true
}