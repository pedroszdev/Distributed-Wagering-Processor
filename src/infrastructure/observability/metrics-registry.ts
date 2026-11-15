/**
 * Registro de métricas no formato de texto do Prometheus (sem dependência externa).
 *
 * Cada instância da API tem os seus contadores em memória; o Prometheus coleta
 * (scrape) o GET /metrics de cada instância e soma. É assim que métricas de
 * processo funcionam: elas NÃO são estado de negócio (isso fica no Postgres).
 */
export type Labels = Record<string, string>;

function labelKey(labels: Labels): string {
  return Object.keys(labels)
    .sort()
    .map((key) => `${key}=${labels[key]}`)
    .join(",");
}

/** Escape de valores de label exigido pelo formato: \ " e quebra de linha. */
function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
}

function formatLabels(labels: Labels): string {
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) return "";
  return `{${keys.map((key) => `${key}="${escapeLabelValue(labels[key]!)}"`).join(",")}}`;
}

abstract class Metric {
  constructor(
    readonly name: string,
    readonly help: string,
    readonly type: "counter" | "gauge" | "histogram",
  ) {}

  abstract samples(): string[];

  render(): string {
    return [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} ${this.type}`, ...this.samples()].join("\n");
  }
}

/** Só sobe. Ex.: total de transações processadas. */
export class Counter extends Metric {
  private readonly values = new Map<string, { labels: Labels; value: number }>();

  constructor(name: string, help: string) {
    super(name, help, "counter");
  }

  inc(labels: Labels = {}, by = 1): void {
    if (by < 0) throw new Error(`counter ${this.name} não pode diminuir`);
    const key = labelKey(labels);
    const current = this.values.get(key);
    this.values.set(key, { labels, value: (current?.value ?? 0) + by });
  }

  get(labels: Labels = {}): number {
    return this.values.get(labelKey(labels))?.value ?? 0;
  }

  samples(): string[] {
    return [...this.values.values()].map(({ labels, value }) => `${this.name}${formatLabels(labels)} ${value}`);
  }
}

/** Sobe e desce, ou é lido na hora. Ex.: idade do evento mais antigo na outbox. */
export class Gauge extends Metric {
  private readonly values = new Map<string, { labels: Labels; value: number }>();

  constructor(name: string, help: string) {
    super(name, help, "gauge");
  }

  set(value: number, labels: Labels = {}): void {
    this.values.set(labelKey(labels), { labels, value });
  }

  get(labels: Labels = {}): number {
    return this.values.get(labelKey(labels))?.value ?? 0;
  }

  samples(): string[] {
    return [...this.values.values()].map(({ labels, value }) => `${this.name}${formatLabels(labels)} ${value}`);
  }
}

/** Distribuição em faixas (buckets). Ex.: latência de processamento. */
export class Histogram extends Metric {
  private readonly series = new Map<string, { labels: Labels; counts: number[]; sum: number; count: number }>();

  constructor(
    name: string,
    help: string,
    private readonly buckets: number[],
  ) {
    super(name, help, "histogram");
  }

  observe(value: number, labels: Labels = {}): void {
    const key = labelKey(labels);
    let serie = this.series.get(key);
    if (!serie) {
      serie = { labels, counts: this.buckets.map(() => 0), sum: 0, count: 0 };
      this.series.set(key, serie);
    }
    this.buckets.forEach((bound, i) => {
      if (value <= bound) serie!.counts[i]!++;
    });
    serie.sum += value;
    serie.count++;
  }

  count(labels: Labels = {}): number {
    return this.series.get(labelKey(labels))?.count ?? 0;
  }

  samples(): string[] {
    const lines: string[] = [];
    for (const { labels, counts, sum, count } of this.series.values()) {
      this.buckets.forEach((bound, i) => {
        lines.push(`${this.name}_bucket${formatLabels({ ...labels, le: String(bound) })} ${counts[i]}`);
      });
      lines.push(`${this.name}_bucket${formatLabels({ ...labels, le: "+Inf" })} ${count}`);
      lines.push(`${this.name}_sum${formatLabels(labels)} ${sum}`);
      lines.push(`${this.name}_count${formatLabels(labels)} ${count}`);
    }
    return lines;
  }
}

export class MetricsRegistry {
  private readonly metrics: Metric[] = [];

  counter(name: string, help: string): Counter {
    return this.register(new Counter(name, help));
  }

  gauge(name: string, help: string): Gauge {
    return this.register(new Gauge(name, help));
  }

  histogram(name: string, help: string, buckets: number[]): Histogram {
    return this.register(new Histogram(name, help, buckets));
  }

  /** O corpo do GET /metrics. */
  render(): string {
    return `${this.metrics.map((metric) => metric.render()).join("\n\n")}\n`;
  }

  private register<T extends Metric>(metric: T): T {
    if (this.metrics.some((m) => m.name === metric.name)) {
      throw new Error(`métrica ${metric.name} registrada duas vezes`);
    }
    this.metrics.push(metric);
    return metric;
  }
}
