//! The controller's Kubernetes client sets up TLS in the same process as the history store's S3
//! client (the Python extension links both). rustls needs exactly one crypto provider there, or
//! it cannot choose and panics on the first TLS client. Built with the whole workspace (as CI
//! tests it, and as the extension is), this fails if a second provider creeps back in.

#[tokio::test]
async fn the_kubernetes_client_sets_up_tls_with_one_crypto_provider() {
    let config = kube::Config::new("https://127.0.0.1:6443".parse().unwrap());
    kube::Client::try_from(config).unwrap();
}
