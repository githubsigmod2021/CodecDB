clear;
load('gen');

plain_slope = int_gen.plain\int_gen.plain_gen;
dict_slope = int_gen.plain\int_gen.dict_gen;
bp_slope = int_gen.plain\int_gen.bp_gen;
rle_slope = int_gen.plain\int_gen.rle_gen;
deltabp_slope = int_gen.plain\int_gen.deltabp_gen;

sample = datasample(int_gen,100);

scatter(sample.plain, sample.plain_gen);
hold on
scatter(sample.plain, sample.bp_gen);
scatter(sample.plain, sample.rle_gen);
scatter(sample.plain, sample.dict_gen);
scatter(sample.plain, sample.deltabp_gen);

plot(sample.plain, sample.plain * plain_slope);
plot(sample.plain, sample.plain * bp_slope);
plot(sample.plain, sample.plain * rle_slope);
plot(sample.plain, sample.plain * dict_slope);
plot(sample.plain, sample.plain * deltabp_slope);

csvwrite('gen_int.data',sample{:,1:6});