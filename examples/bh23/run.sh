xz -v -d -k uniprotkb_reviewed_viruses_10239_0.rdf.xz
riot --output=ntriples uniprotkb_reviewed_viruses_10239_0.rdf > uniprotkb_reviewed_viruses_10239_0.nt
cargo run -r --example bh23