clear
load('plain_int');

enc_gen_slope = intdata.plain\intdata.gen_cpu;
gzip_gen_slope = intdata.plain\intdata.gzip_gen_cpu;
lzo_gen_slope = intdata.plain\intdata.lzo_gen_cpu;

enc_scan_slope = intdata.plain\intdata.scan_cpu;
gzip_scan_slope = intdata.plain\intdata.gzip_scan_cpu;
lzo_scan_slope = intdata.plain\intdata.lzo_scan_cpu;

sample=datasample(intdata,300);

hold on
plot(intdata.plain, intdata.plain*enc_gen_slope);
plot(intdata.plain, intdata.plain*gzip_gen_slope);
plot(intdata.plain, intdata.plain*lzo_gen_slope);

scatter(sample.plain, sample.gen_cpu, '*');
scatter(sample.plain, sample.gzip_gen_cpu, 'o');
scatter(sample.plain, sample.lzo_gen_cpu, '+');

legend('Best Encoding Fitting', 'GZip Fitting','LZO Fitting','Best Encoding','GZip','LZO');

oindex = [1 4 6 9 11 14 16];
output = sample{:,oindex};
csvwrite('comp_time_plain_int.data',output);
