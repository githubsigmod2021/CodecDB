clear;
load('scan');

plain_slope = str_scan.PLAIN_size\str_scan.PLAIN_cpu;
dict_slope = str_scan.PLAIN_size\str_scan.DICT_cpu;
delta_slope = str_scan.PLAIN_size\str_scan.DELTA_cpu;
deltal_slope = str_scan.PLAIN_size\str_scan.DELTAL_cpu;

sample = datasample(str_scan,100);

scatter(sample.PLAIN_size, sample.PLAIN_cpu);
hold on
scatter(sample.PLAIN_size, sample.DELTAL_cpu);
scatter(sample.PLAIN_size, sample.DELTA_cpu);
scatter(sample.PLAIN_size, sample.DICT_cpu);

plot(sample.PLAIN_size, sample.PLAIN_size * plain_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * delta_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * deltal_slope);
plot(sample.PLAIN_size, sample.PLAIN_size * dict_slope);

csvwrite('scan_str.data',sample{:,1:8});