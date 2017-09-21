# Datadog Agent listener service input plugin

这个可以接受datadog的探针，然后把数据转入influxdb产品线

### Configuration:

This is a sample configuration for the plugin.

```toml
# # Influx Datadog Agent listener
[[inputs.ddagent_listener]]
  ## Address and port to host Datadog Agent listener on
  service_address = ":8186"

  ## timeouts
  read_timeout = "10s"
  write_timeout = "10s"

  ## HTTPS
  tls_cert= "/etc/telegraf/cert.pem"
  tls_key = "/etc/telegraf/key.pem"

  ## MTLS
  tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]
```
