wget https://dtdr.sdsc.edu/static/images/hpa_omero.ttl.gz
gunzip hpa_omero.ttl.gz
riot --output=ntriples hpa_omero.ttl > hpa_omero.nt
cargo run -r --example zarr 