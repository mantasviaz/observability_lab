import json, os, random, time, logging
from functools import lru_cache
from flask import Flask, request, jsonify

# setup open telem
from opentelemetry import trace, metrics
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "observability-lab")
ENV = os.getenv("DEPLOY_ENV", "dev")
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://agent:4317")

resource = Resource.create({"service.name": SERVICE_NAME, "deployment.environment": ENV})

# tracing
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)))
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)

# metrics
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTLP_ENDPOINT, insecure=True),
    export_interval_millis=5000,
)
metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
meter = metrics.get_meter(__name__)
req_counter = meter.create_counter("requests_total")
latency_hist = meter.create_histogram("request_latency_ms")

# logging out
logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(handler)

app = Flask(__name__)

def log_json(event, **kwargs):
    payload = {"event": event, "service": SERVICE_NAME, "env": ENV, **kwargs}
    logger.info(json.dumps(payload))

@app.before_request
def _start_timer():
    request._start = time.time()

@app.after_request
def _after(resp):
    try:
        dur_ms = int((time.time() - getattr(request, "_start", time.time())) * 1000)
        req_counter.add(1, {"path": request.path, "method": request.method, "status": str(resp.status_code)})
        latency_hist.record(dur_ms, {"path": request.path, "method": request.method})
        span = trace.get_current_span()
        trace_id = getattr(span.get_span_context(), "trace_id", 0)
        log_json(
            "request",
            path=request.path,
            method=request.method,
            status=resp.status_code,
            latency_ms=dur_ms,
            trace_id=hex(trace_id),
        )
    except Exception:
        pass
    return resp

@app.get("/health")
def health():
    return jsonify(ok=True, service=SERVICE_NAME, env=ENV), 200

@app.get("/hello")
def hello():
    delay_ms = int(request.args.get("delay_ms", "0"))
    if delay_ms > 0:
        time.sleep(delay_ms / 1000)
    return jsonify(message="hello, datadog"), 200

@lru_cache(maxsize=256)
def expensive_compute_cached(x):
    time.sleep(0.8)
    return x * x

def expensive_compute_uncached(x):
    time.sleep(0.8)
    return x * x

@app.get("/slow")
def slow():
    cache_on = request.args.get("cache", "0") == "1"
    x = int(request.args.get("x", "7"))
    with tracer.start_as_current_span("slow_work"):
        if cache_on:
            res = expensive_compute_cached(x)
        else:
            res = expensive_compute_uncached(x)
        return jsonify(result=res, cache=cache_on), 200

@app.get("/error")
def sometimes_errors():
    rate = float(request.args.get("rate", "0.2"))
    if random.random() < rate:
        return jsonify(error="intentional failure"), 500
    return jsonify(ok=True), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
