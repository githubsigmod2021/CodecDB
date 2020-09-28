clear;
load('scan');

plain_slope = int_scan.PLAIN_size\int_scan.PLAIN_cpu;
dict_slope = int_scan.PLAIN_size\int_scan.DICT_cpu;
bp_slope = int_scan.PLAIN_size\int_scan.BP_cpu;
rle_slope = int_scan.PLAIN_size\int_scan.RLE_cpu;
deltabp_slope = int_scan.PLAIN_size\int_scan.DELTABP_cpu;

sample = datasample(int_scan,100);

scatter(sample.PLAIN_size, sample.PLAIN_cpu);
hold on
scatter(sample.PLAIN_size, sample.BP_cpu);
scatter(sample.PLAIN_size, sample.RLE_cpu);
scatter(sample.PLAIN_size, sample.DICT_cpu);
scatter(sample.PLAIN_size, sample.DELTABP_cpu);

plot(sample.PLAIN_size, sample.PLAIN_size * plain_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * bp_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * rle_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * dict_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * deltabp_slope);

csvwrite('scan_int.data',sample{:,1:10});