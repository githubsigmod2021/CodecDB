clear;
load('gen');

plain_slope = str_gen.plain\str_gen.plain_gen;
dict_slope = str_gen.plain\str_gen.dict_gen;
delta_slope = str_gen.plain\str_gen.delta_gen;
deltal_slope = str_gen.plain\str_gen.deltal_gen;

sample = datasample(str_gen,100);

scatter(sample.plain, sample.plain_gen);
hold on
scatter(sample.plain, sample.delta_gen);
scatter(sample.plain, sample.deltal_gen);
scatter(sample.plain, sample.dict_gen);

plot(sample.plain, sample.plain * plain_slope);
plot(sample.plain, sample.plain * delta_slope);
plot(sample.plain, sample.plain * deltal_slope);
plot(sample.plain, sample.plain * dict_slope);

csvwrite('gen_str.data',sample{:,1:5});