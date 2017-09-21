package ddagent_listener

import (
	"compress/zlib"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
	"encoding/json"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	"github.com/influxdata/telegraf/selfstat"
	"bufio"
	"strings"
	"math"
	"fmt"
	"strconv"
)



type DDAgentListener struct {
	ServiceAddress string
	ReadTimeout    internal.Duration
	WriteTimeout   internal.Duration
	MaxBodySize    int64
	MaxLineSize    int
	Port           int

	TlsAllowedCacerts []string
	TlsCert           string
	TlsKey            string

	mu sync.Mutex
	wg sync.WaitGroup

	listener net.Listener

	parser influx.InfluxParser
	acc    telegraf.Accumulator

	RequestsRecv    selfstat.Stat
	RequestsServed  selfstat.Stat

	PingsServed     selfstat.Stat
	PingsRecv       selfstat.Stat

	IntakeServed     selfstat.Stat
	IntakeRecv       selfstat.Stat

	SeriesServed     selfstat.Stat
	SeriesRecv       selfstat.Stat


	NotFoundsServed selfstat.Stat
}

const sampleConfig = `
  ## Address and port to host HTTP listener on
  service_address = ":8081"

  ## maximum duration before timing out read of the request
  read_timeout = "10s"
  ## maximum duration before timing out write of the response
  write_timeout = "10s"

  ## Set one or more allowed client CA certificate file names to 
  ## enable mutually authenticated TLS connections
  tls_allowed_cacerts = ["/etc/telegraf/clientca.pem"]

  ## Add service certificate and key
  tls_cert = "/etc/telegraf/cert.pem"
  tls_key = "/etc/telegraf/key.pem"
`

func (h *DDAgentListener) SampleConfig() string {
	return sampleConfig
}

func (h *DDAgentListener) Description() string {
	return "Influx datadog agent listener"
}

func (h *DDAgentListener) Gather(_ telegraf.Accumulator) error {
	return nil
}

// Start starts the http listener service.
func (h *DDAgentListener) Start(acc telegraf.Accumulator) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	tags := map[string]string{
		"address": h.ServiceAddress,
	}

	h.RequestsServed = selfstat.Register("ddagent_listener", "requests_served", tags)
	h.RequestsRecv = selfstat.Register("ddagent_listener", "requests_received", tags)

	h.IntakeServed = selfstat.Register("ddagent_listener", "intake_served", tags)
	h.IntakeRecv = selfstat.Register("ddagent_listener", "intake_received", tags)


	h.SeriesServed = selfstat.Register("ddagent_listener", "series_served", tags)
	h.SeriesRecv = selfstat.Register("ddagent_listener", "series_received", tags)

	h.PingsServed = selfstat.Register("ddagent_listener", "pings_served", tags)
	h.PingsRecv = selfstat.Register("ddagent_listener", "pings_received", tags)
	h.NotFoundsServed = selfstat.Register("ddagent_listener", "not_founds_served", tags)

	if h.ReadTimeout.Duration < time.Second {
		h.ReadTimeout.Duration = time.Second * 10
	}
	if h.WriteTimeout.Duration < time.Second {
		h.WriteTimeout.Duration = time.Second * 10
	}

	h.acc = acc

	tlsConf := h.getTLSConfig()

	server := &http.Server{
		Addr:         h.ServiceAddress,
		Handler:      h,
		ReadTimeout:  h.ReadTimeout.Duration,
		WriteTimeout: h.WriteTimeout.Duration,
		TLSConfig:    tlsConf,
	}

	var err error
	var listener net.Listener
	if tlsConf != nil {
		listener, err = tls.Listen("tcp", h.ServiceAddress, tlsConf)
	} else {
		listener, err = net.Listen("tcp", h.ServiceAddress)
	}
	if err != nil {
		return err
	}
	h.listener = listener
	h.Port = listener.Addr().(*net.TCPAddr).Port

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		server.Serve(h.listener)
	}()

	log.Printf("I! Started HTTP listener service on %s\n", h.ServiceAddress)

	return nil
}

// Stop cleans up all resources
func (h *DDAgentListener) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.listener.Close()
	h.wg.Wait()

	log.Println("I! Stopped HTTP listener service on ", h.ServiceAddress)
}

func (h *DDAgentListener) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	h.RequestsRecv.Incr(1)
	defer h.RequestsServed.Incr(1)
	switch req.URL.Path {
	case "/api/v1/series/":
		h.IntakeRecv.Incr(1)
		defer h.IntakeServed.Incr(1)
		h.serveSeries(res, req)
	case "/intake/":
		h.SeriesRecv.Incr(1)
		defer h.SeriesServed.Incr(1)
		h.serveIntake(res, req)
	/*case "/write":
		h.WritesRecv.Incr(1)
		defer h.WritesServed.Incr(1)
		h.serveWrite(res, req)
	case "/query":
		h.QueriesRecv.Incr(1)
		defer h.QueriesServed.Incr(1)
		// Deliver a dummy response to the query endpoint, as some InfluxDB
		// clients test endpoint availability with a query
		res.Header().Set("Content-Type", "application/json")
		res.Header().Set("X-Influxdb-Version", "1.0")
		res.WriteHeader(http.StatusOK)
		res.Write([]byte("{\"results\":[]}"))*/
	case "/ping":
		h.PingsRecv.Incr(1)
		defer h.PingsServed.Incr(1)
		// respond to ping requests
		res.WriteHeader(http.StatusNoContent)
	default:
		defer h.NotFoundsServed.Incr(1)
		// Don't know how to respond to calls to other endpoints
		http.NotFound(res, req)
	}
}

type Serie struct {
	Metric string `json:"metric"`
	Tags []string `json:"tags,omitempty"`
	Interval float64 `json:"interval"`
	DeviceName string `json:"device_name,omitempty"`
	Device string `json:"device"`
	Host string `json:"host"`
	Points [][]float64 `json:"points"`
	Type string `json:"type"`
}

type Series struct {
	Series []Serie `json:"series"`
}

type Intake struct {
	Metric []interface{} `json:"metric"`
}

func (h *DDAgentListener) serveIntake(w http.ResponseWriter, r *http.Request) {
	// Check that the content length is not too large for us to handle.
	defer  r.Body.Close()

	// Require POST method.
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	// Wrap reader if it's gzip encoded.
	var br *bufio.Reader
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "could not read gzip, "+err.Error(), http.StatusBadRequest)
			return
		}

		br = bufio.NewReader(zr)
	} else if r.Header.Get("Content-Encoding") == "deflate" {
		zr, err := zlib.NewReader(r.Body)
		if err != nil {
			http.Error(w, "could not read gzip, "+err.Error(), http.StatusBadRequest)
			return
		}

		br = bufio.NewReader(zr)
	} else {
		br = bufio.NewReader(r.Body)
	}




	dec := json.NewDecoder(br)


	intake := struct {
		Load1 float64 `json:"system.load.1"`
		Load5 float64 `json:"system.load.5"`
		Load15 float64 `json:"system.load.15"`
		Load_norm_1 float64 `json:"system.load.norm.1"`
		Load_norm_2 float64 `json:"system.load.norm.2"`
		Load_norm_3 float64 `json:"system.load.norm.3"`
		Uptime float64 `json:"system.uptime"`
		Host string `json:"internalHostname"`
		CpuUser float64 `json:"cpuUser"`//用户进程占用率
		CpuIdle float64 `json:"cpuIdle"`
		CpuSystem float64 `json:"cpuSystem"`//系统进程占有率
		CpuWait float64 `json:"cpuWait"`
		CpuStolen float64 `json:"cpuStolen"`

		CpuGuest float64 `json:"cpuGuest"`
		Metrics [][]interface{} `json:"metrics"`

		EmPhysUsed float64 `json:"emPhysUsed"`
		MemPhysPctUsable float64 `json:"memPhysPctUsable"`
		MemPhysFree float64 `json:"memPhysFree"`
		MemPhysTotal float64 `json:"memPhysTotal"`
		MemPhysUsable float64 `json:"memPhysUsable"`

		MemSwapUsed float64 `json:"memSwapUsed"`
		MemCached float64 `json:"memCached"`
		MemSwapFree float64 `json:"memSwapFree"`
		MemSwapPctFree float64 `json:"memSwapPctFree"`
		MemSwapTotal float64 `json:"memSwapTotal"`

		MemBuffers float64 `json:"memBuffers"`
		MemShared float64 `json:"memShared"`
		MemSlab float64 `json:"memSlab"`
		MemPageTables float64 `json:"memPageTables"`
		MemSwapCached float64 `json:"memSwapCached"`

		CollectionTimestamp float64 `json:"collection_timestamp"`
		IoStats map[string]map[string]interface{} `json:"ioStats"`
		FileHandleInUse float64 `json:"system.fs.file_handles.in_use"`
	}{}

	if err := dec.Decode(&intake); err != nil {
		http.Error(w, "json array decode error", http.StatusBadRequest)
		return
	}

	if intake.Host == "datadog-177" {
		fmt.Println(intake.Host)
	}


	var metricTime time.Time
	//var metricArr []telegraf.Metric
	for _, ometric := range intake.Metrics  {
		tags := map[string]string{}
		metrics := strings.Split((ometric[0]).(string),".")
		var field string
		field = strings.Join(metrics[1:],"_")
		fields := map[string]interface{}{
			field: ometric[2].(float64),
		}
		//sec,nasec := math.Modf(serie.Points[0][0])
		if metricTime.IsZero()  {
			metricTime = time.Unix(int64(ometric[1].(float64)),0)
		}
		//now := time.Unix(int64(ometric[1].(float64)),0)
		tags["host"] = intake.Host
		var originTags map[string]interface{}
		originTags = ometric[3].(map[string]interface{})
		if originTags["device_name"] != nil {
			tags["device"] = originTags["device_name"].(string)
		}


		if originTags["tags"] != nil && len(originTags["tags"].([]string)) > 0 {
			for _, tag := range originTags["tags"].([]string) {
				fmt.Println(tag)
				tmps := strings.Split(tag,":")
				if(len(tmps) == 2 &&  tmps[0] == "instance" && tmps[1] != ""){
					tags[tmps[0]] = tmps[1]
				}
			}
		}

		m, err2 := metric.New(metrics[0], tags, fields, metricTime)
		if err2 != nil {
			fmt.Println(err2)
		} else {
			//metricArr = append(metricArr, m)
			h.acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())
		}
		//fmt.Println(m)

	}
	tags := map[string]string{}
	tags["host"] = intake.Host

	if metricTime.IsZero()  {
		sec,nasec := math.Modf(intake.CollectionTimestamp)
		metricTime = time.Unix(int64(sec),int64(nasec * 1000 * 1000 * 1000))
	}

	//uptime
	{

		fields := map[string]interface{}{
			"uptime": intake.Uptime,
		}
		h.acc.AddFields("system", fields, tags, metricTime)
	}


	{

		fields := map[string]interface{}{
			"fs_file_handles_in_use": intake.FileHandleInUse,
		}
		h.acc.AddFields("system", fields, tags, metricTime)
	}



	//IoStats
	for key,mapIo := range intake.IoStats {
		//fmt.Println(key)
		ioTags := map[string]string{}
		ioTags["host"] = intake.Host
		ioTags["device"] = key
		fields := map[string]interface{}{}

		for key2,iostate := range mapIo {
			//fmt.Println(key2)
			///fmt.Println(iostate)
			value, ok := iostate.(string)
			if !ok {
				value, ok := iostate.(float64)
				if !ok {
					fmt.Println(key2,iostate)
				} else {
					fields[key2] = value
					h.acc.AddFields("io", fields, ioTags, metricTime)
				}

			} else {
				i, err := strconv.ParseFloat(value, 64)
				if err != nil {
					fmt.Println(err)
				} else {
					fields[key2] = i
					h.acc.AddFields("io", fields, ioTags, metricTime)
				}
			}




		}

	}

	//load
	{
		/*
		"load1":   loadavg.Load1,
		"load5":   loadavg.Load5,
		"load15":  loadavg.Load15,
		"n_users": len(users),
		"n_cpus":  runtime.NumCPU(),

		*/


		fields := map[string]interface{}{
			"load1": intake.Load1,
			"load5": intake.Load5,
			"load15": intake.Load15,
			"Load_norm_1": intake.Load_norm_1,
			"Load_norm_2": intake.Load_norm_2,
			"Load_norm_3": intake.Load_norm_3,

		}
		h.acc.AddFields("system", fields, tags, metricTime)
	}

	{
		cpuTags := map[string]string{}
		cpuTags["host"] = intake.Host
		cpuTags["cpu"] = "cpu-total"

		fields := map[string]interface{}{
			"usage_user": intake.CpuUser,
			"usage_idle": intake.CpuIdle,
			"usage_system": intake.CpuSystem,
			"usage_iowait": intake.CpuWait,
			"usage_steal": intake.CpuStolen,
			"usage_guest": intake.CpuGuest,
		}
		h.acc.AddFields("cpu", fields, cpuTags, metricTime)
	}

	/*
	"system.mem.used" => "emPhysUsed",
            "system.mem.pct_usable" => "memPhysPctUsable",
            "system.mem.free" => "memPhysFree",
            "system.mem.total" => "memPhysTotal",
            "system.mem.usable" => "memPhysUsable",
            "system.swap.used" => "memSwapUsed",

            "system.mem.cached" => "memCached",
            "system.swap.free" => "memSwapFree",
            "system.swap.pct_free" => "memSwapPctFree",
            "system.swap.total" => "memSwapTotal",
            "system.mem.buffered" => "memBuffers",


            "system.mem.shared" => "memShared",
            "system.mem.slab" => "memSlab",
            "system.mem.page_tables" => "memPageTables",
            "system.swap.cached" => "memSwapCached"
            */
	{
		fields := map[string]interface{}{
			"emPhysUsed": intake.EmPhysUsed,
			"memPhysPctUsable": intake.MemPhysPctUsable,
			"memPhysFree": intake.MemPhysFree,
			"memPhysTotal": intake.MemPhysTotal,
			"memPhysUsable": intake.MemPhysUsable,

			"memSwapUsed": intake.MemSwapUsed,
			"memCached": intake.MemCached,
			"memSwapFree": intake.MemSwapFree,
			"memSwapPctFree": intake.MemSwapPctFree,
			"memSwapTotal": intake.MemSwapTotal,

			"memBuffers": intake.MemBuffers,
			"memShared": intake.MemShared,
			"memSlab": intake.MemSlab,
			"memPageTables": intake.MemPageTables,
			"memSwapCached": intake.MemSwapCached,
		}
		h.acc.AddFields("mem", fields, tags, metricTime)
	}
}

func (h *DDAgentListener) serveSeries(w http.ResponseWriter, r *http.Request) {
	// Check that the content length is not too large for us to handle.
	defer  r.Body.Close()

	// Require POST method.
	if r.Method != "POST" {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	// Wrap reader if it's gzip encoded.
	var br *bufio.Reader
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "could not read gzip, "+err.Error(), http.StatusBadRequest)
			return
		}

		br = bufio.NewReader(zr)
	} else if r.Header.Get("Content-Encoding") == "deflate" {
		zr, err := zlib.NewReader(r.Body)
		if err != nil {
			http.Error(w, "could not read gzip, "+err.Error(), http.StatusBadRequest)
			return
		}

		br = bufio.NewReader(zr)
	} else {
		br = bufio.NewReader(r.Body)
	}
	dec := json.NewDecoder(br)
	series := Series{}
	if err := dec.Decode(&series); err != nil {
		http.Error(w, "json array decode error", http.StatusBadRequest)
		return
	}

	//fmt.Println("%v",series)
	var metricArr []telegraf.Metric
	for _, serie := range series.Series  {
		tags := map[string]string{}
		metrics := strings.Split(serie.Metric,".")
		var field string
		field = strings.Join(metrics[1:],"_")
		fields := map[string]interface{}{
			field: serie.Points[0][1],
		}
		sec,nasec := math.Modf(serie.Points[0][0])
		now := time.Unix(int64(sec),int64(nasec * 1000 * 1000 * 1000))
		tags["host"] = serie.Host
		if serie.DeviceName != "" {
			tags["device"] = serie.DeviceName
		}

		if serie.Device != "" {
			tags["device"] = serie.Device
		}

		if serie.Tags != nil && len(serie.Tags) > 0 {
			for _, tag := range serie.Tags {
				fmt.Println(tag)
				tmps := strings.Split(tag,":")
				if(len(tmps) == 2 &&  tmps[0] == "instance" && tmps[1] != ""){
					tags[tmps[0]] = tmps[1]
				}
			}
		}

		m, err2 := metric.New(metrics[0], tags, fields, now)
		if err2 != nil {
			fmt.Println(err2)
		} else {
			metricArr = append(metricArr, m)
		}
		//fmt.Println(m)
		h.acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())


		{
			cpuTags := map[string]string{}
			cpuTags["host"] = serie.Host
			cpuTags["cpu"] = "cpu-total"

			if serie.Metric ==  "system.cpu.system" {
				fields := map[string]interface{}{
					"usage_system": serie.Points[0][1],
				}
				h.acc.AddFields("cpu", fields, cpuTags,  m.Time())
			} else if serie.Metric ==  "system.cpu.user" {
				fields := map[string]interface{}{
					"usage_user": serie.Points[0][1],
				}
				h.acc.AddFields("cpu", fields, cpuTags,  m.Time())
			} else if serie.Metric ==  "system.cpu.idle" {
				fields := map[string]interface{}{
					"usage_idle": serie.Points[0][1],
				}
				h.acc.AddFields("cpu", fields, cpuTags,  m.Time())
			}



		}
	}


}

func (h *DDAgentListener) parse(b []byte, t time.Time, precision string) error {
	metrics, err := h.parser.ParseWithDefaultTimePrecision(b, t, precision)

	for _, m := range metrics {
		h.acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())
	}

	return err
}

func tooLarge(res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("X-Influxdb-Version", "1.0")
	res.WriteHeader(http.StatusRequestEntityTooLarge)
	res.Write([]byte(`{"error":"http: request body too large"}`))
}

func badRequest(res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.Header().Set("X-Influxdb-Version", "1.0")
	res.WriteHeader(http.StatusBadRequest)
	res.Write([]byte(`{"error":"http: bad request"}`))
}

func (h *DDAgentListener) getTLSConfig() *tls.Config {
	tlsConf := &tls.Config{
		InsecureSkipVerify: false,
		Renegotiation:      tls.RenegotiateNever,
	}

	if len(h.TlsCert) == 0 || len(h.TlsKey) == 0 {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(h.TlsCert, h.TlsKey)
	if err != nil {
		return nil
	}
	tlsConf.Certificates = []tls.Certificate{cert}

	if h.TlsAllowedCacerts != nil {
		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
		clientPool := x509.NewCertPool()
		for _, ca := range h.TlsAllowedCacerts {
			c, err := ioutil.ReadFile(ca)
			if err != nil {
				continue
			}
			clientPool.AppendCertsFromPEM(c)
		}
		tlsConf.ClientCAs = clientPool
	}

	return tlsConf
}

func init() {
	inputs.Add("ddagent_listener", func() telegraf.Input {
		return &DDAgentListener{
			ServiceAddress: ":8081",
		}
	})
}
