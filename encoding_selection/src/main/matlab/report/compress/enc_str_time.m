clear
load('enc');

enc_gen_slope = str_enc.plain\str_enc.gen_wc;
gzip_gen_slope = str_enc.plain\str_enc.gzip_gen_wc;
lzo_gen_slope = str_enc.plain\str_enc.lzo_gen_wc;

enc_scan_slope = str_enc.plain\str_enc.scan_cpu;
gzip_scan_slope = str_enc.plain\str_enc.gzip_scan_cpu;
lzo_scan_slope = str_enc.plain\str_enc.lzo_scan_cpu;

sample=datasample(str_enc,300);

hold on
plot(str_enc.plain, str_enc.plain*enc_gen_slope);
plot(str_enc.plain, str_enc.plain*gzip_gen_slope);
plot(str_enc.plain, str_enc.plain*lzo_gen_slope);

scatter(sample.plain, sample.gen_cpu, '*');
scatter(sample.plain, sample.gzip_gen_cpu, 'o');
scatter(sample.plain, sample.lzo_gen_cpu, '+');

legend('Best Encoding Fitting', 'GZip Fitting','LZO Fitting','Best Encoding','GZip','LZO');

oindex = [1 4 6 9 11 14 16];
output = sample{:,oindex};
csvwrite('comp_time_enc_str.data',output);
