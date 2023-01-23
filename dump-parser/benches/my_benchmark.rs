use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dump_parser::postgres;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("postgres tokenizer 1", |b| b.iter(|| {
        let q = format!(
            "INSERT INTO public.test (positive_number, negative_number, long_number) VALUES (+{}, {}, {}L);",
            black_box(20),
            black_box(20),
            black_box(20)
        );

        postgres::Tokenizer::new(q.as_str()).tokenize().unwrap()
    }));

    c.bench_function("postgres tokenizer 2", |b| {
        b.iter(|| {
            let q = format!(
                "INSERT INTO public.test (positive_number) VALUES (+{});",
                black_box(20),
            );

            postgres::Tokenizer::new(q.as_str()).tokenize().unwrap()
        })
    });

    c.bench_function("postgres tokenizer 3", |b| {
        b.iter(|| {
            let q = format!(
                "INSERT INTO public.test (positive_number, negative_number, long_number, positive_number, negative_number, long_number, positive_number, negative_number, long_number) VALUES (+{}, {}, {}L, +{}, {}, {}L, +{}, {}, {}L);",
                black_box(20),
                black_box(20),
                black_box(20),
                black_box(20),
                black_box(20),
                black_box(20),
                black_box(20),
                black_box(20),
                black_box(20),
            );


            postgres::Tokenizer::new(q.as_str()).tokenize().unwrap()
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
