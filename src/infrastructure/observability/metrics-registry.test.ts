import { describe, expect, test } from "bun:test";
import { MetricsRegistry } from "./metrics-registry.ts";

describe("Counter", () => {
  test("começa em 0, soma por combinação de labels e renderiza no formato do Prometheus", () => {
    const registry = new MetricsRegistry();
    const counter = registry.counter("jobs_total", "Jobs executados.");

    counter.inc({ result: "ok" });
    counter.inc({ result: "ok" });
    counter.inc({ result: "error" }, 3);

    expect(counter.get({ result: "ok" })).toBe(2);
    expect(counter.get({ result: "error" })).toBe(3);
    expect(counter.get({ result: "nunca" })).toBe(0);
    expect(registry.render()).toBe(
      [
        "# HELP jobs_total Jobs executados.",
        "# TYPE jobs_total counter",
        'jobs_total{result="ok"} 2',
        'jobs_total{result="error"} 3',
        "",
      ].join("\n"),
    );
  });

  test("a ordem das labels não importa (é a mesma série)", () => {
    const counter = new MetricsRegistry().counter("x_total", "x");
    counter.inc({ a: "1", b: "2" });
    counter.inc({ b: "2", a: "1" });
    expect(counter.get({ a: "1", b: "2" })).toBe(2);
  });

  test("sem labels", () => {
    const registry = new MetricsRegistry();
    registry.counter("y_total", "y").inc();
    expect(registry.render()).toContain("\ny_total 1\n");
  });

  test("não pode diminuir", () => {
    expect(() => new MetricsRegistry().counter("z_total", "z").inc({}, -1)).toThrow();
  });

  test("escapa aspas, barra e quebra de linha nos valores das labels", () => {
    const registry = new MetricsRegistry();
    registry.counter("e_total", "e").inc({ reason: 'diz "oi"\\\n' });
    expect(registry.render()).toContain('e_total{reason="diz \\"oi\\"\\\\\\n"} 1');
  });
});

describe("Gauge", () => {
  test("guarda o último valor", () => {
    const registry = new MetricsRegistry();
    const gauge = registry.gauge("lag_seconds", "Lag.");
    gauge.set(5);
    gauge.set(2);
    expect(gauge.get()).toBe(2);
    expect(registry.render()).toContain("# TYPE lag_seconds gauge\nlag_seconds 2");
  });
});

describe("Histogram", () => {
  test("conta por bucket (cumulativo), +Inf, soma e total", () => {
    const registry = new MetricsRegistry();
    const histogram = registry.histogram("latency_seconds", "Latência.", [0.1, 0.5, 1]);

    for (const value of [0.05, 0.2, 0.3, 0.7, 2]) histogram.observe(value, { op: "bet" });

    expect(histogram.count({ op: "bet" })).toBe(5);
    const text = registry.render();
    expect(text).toContain('latency_seconds_bucket{le="0.1",op="bet"} 1');
    expect(text).toContain('latency_seconds_bucket{le="0.5",op="bet"} 3');
    expect(text).toContain('latency_seconds_bucket{le="1",op="bet"} 4');
    expect(text).toContain('latency_seconds_bucket{le="+Inf",op="bet"} 5');
    expect(text).toContain('latency_seconds_sum{op="bet"} 3.25');
    expect(text).toContain('latency_seconds_count{op="bet"} 5');
  });
});

describe("MetricsRegistry", () => {
  test("o mesmo nome duas vezes é erro (evita duas métricas brigando pelo mesmo nome)", () => {
    const registry = new MetricsRegistry();
    registry.counter("dup_total", "a");
    expect(() => registry.gauge("dup_total", "b")).toThrow();
  });
});
