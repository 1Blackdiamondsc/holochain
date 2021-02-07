use crate::*;
use futures::stream::StreamExt;
use ghost_actor::dependencies::tracing;
use kitsune_p2p_types::dependencies::spawn_pressure;
use kitsune_p2p_types::metrics::metric_task_warn_limit;

fn init_tracing() {
    let _ = tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    );
}

#[tokio::test(threaded_scheduler)]
async fn tls_server_and_client() {
    init_tracing();
    if let Err(e) = tls_server_and_client_inner().await {
        panic!("{:?}", e);
    }
}

#[tokio::test(threaded_scheduler)]
async fn tls_rate() {
    if std::env::var_os("RUST_LOG").is_some() {
        observability::init_fmt(observability::Output::DeadLock)
            .expect("Failed to start contextual logging");
        tokio::spawn(async {
            loop {
                observability::tick_deadlock_catcher();
                tokio::time::delay_for(std::time::Duration::from_secs(1)).await;
            }
        });
    }
    if let Err(e) = tls_rate_inner().await {
        panic!("{:?}", e);
    }
}

async fn tls_server_and_client_inner() -> TransportResult<()> {
    tracing::warn!("start test");

    let tls_config_1 = TlsConfig::new_ephemeral().await?;
    let tls_config_2 = TlsConfig::new_ephemeral().await?;

    let (tls_srv_conf, _tls_cli_conf) = gen_tls_configs(&tls_config_1)?;
    let (_tls_srv_conf, tls_cli_conf) = gen_tls_configs(&tls_config_2)?;

    let (in_con_send, mut in_con_recv) = futures::channel::mpsc::channel::<TransportEvent>(10);

    metric_task_warn_limit(spawn_pressure::spawn_limit!(1000), async move {
        while let Some(evt) = in_con_recv.next().await {
            match evt {
                TransportEvent::IncomingChannel(_url, mut send, recv) => {
                    tracing::warn!("incoming channel - reading...");
                    let data = recv.read_to_end().await;
                    let data = String::from_utf8_lossy(&data);
                    let data = format!("echo: {}", data);
                    tracing::warn!("incoming channel - responding...");
                    send.write_and_close(data.into_bytes()).await?;
                    tracing::warn!("incoming channel - responding complete.");
                }
            }
        }
        TransportResult::Ok(())
    });

    let (srv_proxy_send, cli_proxy_recv) = futures::channel::mpsc::channel(10);
    let (cli_proxy_send, srv_proxy_recv) = futures::channel::mpsc::channel(10);

    tls_srv::spawn_tls_server(
        "srv".to_string(),
        url2::url2!("srv://srv.srv"),
        tls_srv_conf,
        in_con_send,
        srv_proxy_send,
        srv_proxy_recv,
    )
    .await;

    let ((cli_data_send1, cli_data_recv1), (mut cli_data_send2, cli_data_recv2)) =
        kitsune_p2p_types::transport::create_transport_channel_pair();

    let expected_proxy_url = ProxyUrl::new("srv://srv.srv", tls_config_1.cert_digest)?;
    tls_cli::spawn_tls_client(
        "cli".to_string(),
        expected_proxy_url,
        tls_cli_conf,
        cli_data_send1,
        cli_data_recv1,
        cli_proxy_send,
        cli_proxy_recv,
    )
    .await;

    tracing::warn!("about to write");
    let large_msg = std::iter::repeat(b"a"[0]).take(70_400).collect::<Vec<_>>();
    cli_data_send2.write_and_close(large_msg.clone()).await?;

    tracing::warn!("about to recv");
    let res = cli_data_recv2.collect::<Vec<_>>().await;
    let res = res.into_iter().flat_map(|a| a).collect::<Vec<_>>();
    let data = String::from_utf8_lossy(&res);
    assert_eq!(data.len(), 70_406);
    assert_eq!(
        format!("echo: {}", String::from_utf8_lossy(&large_msg)),
        data
    );

    tracing::warn!("end test");

    Ok(())
}

async fn tls_rate_inner() -> TransportResult<()> {
    tracing::warn!("start test");

    let tls_config_1 = TlsConfig::new_ephemeral().await?;
    let tls_config_2 = TlsConfig::new_ephemeral().await?;

    let (tls_srv_conf, _tls_cli_conf) = gen_tls_configs(&tls_config_1)?;
    let (_tls_srv_conf, tls_cli_conf) = gen_tls_configs(&tls_config_2)?;

    let (in_con_send, mut in_con_recv) = futures::channel::mpsc::channel::<TransportEvent>(10);

    metric_task_warn_limit(spawn_pressure::spawn_limit!(1000), async move {
        while let Some(evt) = in_con_recv.next().await {
            match evt {
                TransportEvent::IncomingChannel(_url, mut send, recv) => {
                    tracing::warn!("incoming channel - reading...");
                    let data = recv.read_to_end().await;
                    let data = String::from_utf8_lossy(&data);
                    let data = format!("echo: {}", data);
                    tracing::warn!("incoming channel - responding...");
                    send.write_and_close(data.into_bytes()).await?;
                    tracing::warn!("incoming channel - responding complete.");
                }
            }
        }
        TransportResult::Ok(())
    });

    let mut num = 0;
    let size = b"hello".to_vec().len();
    let t = std::time::Instant::now();
    loop {
        num += 1;
        let t2 = std::time::Instant::now();

        let (srv_proxy_send, cli_proxy_recv) = futures::channel::mpsc::channel(10);
        let (cli_proxy_send, srv_proxy_recv) = futures::channel::mpsc::channel(10);

        tls_srv::spawn_tls_server(
            "srv".to_string(),
            url2::url2!("srv://srv.srv"),
            tls_srv_conf.clone(),
            in_con_send.clone(),
            srv_proxy_send,
            srv_proxy_recv,
        )
        .await;

        let ((cli_data_send1, cli_data_recv1), (mut cli_data_send2, cli_data_recv2)) =
            kitsune_p2p_types::transport::create_transport_channel_pair();

        let expected_proxy_url = ProxyUrl::new("srv://srv.srv", tls_config_1.cert_digest.clone())?;
        tls_cli::spawn_tls_client(
            "cli".to_string(),
            expected_proxy_url,
            tls_cli_conf.clone(),
            cli_data_send1,
            cli_data_recv1,
            cli_proxy_send,
            cli_proxy_recv,
        )
        .await;

        cli_data_send2.write_and_close(b"hello".to_vec()).await?;

        let resp = cli_data_recv2.read_to_end().await;

        let size = resp.len() + size * 8;
        let el = t.elapsed();
        let avg = el / num;
        let latency = t2.elapsed();
        let mps = num.checked_div(el.as_secs() as u32).unwrap_or(0);
        let mut mbps = (size * num as usize)
            .checked_div(el.as_secs() as usize)
            .unwrap_or(0);
        mbps /= 1000;

        // println!(
        //     "messages per s: {}, {}Mbps, latency {:?}, avg latency {:?}, total {}, el {}",
        //     mps,
        //     mbps,
        //     latency,
        //     avg,
        //     num,
        //     el.as_secs()
        // );
        assert_eq!("echo: hello", &String::from_utf8_lossy(&resp));
    }

}
