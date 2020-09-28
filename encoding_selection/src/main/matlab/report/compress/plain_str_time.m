clear
load('plain_str');

enc_gen_slope = enc_gen_scan.plain\enc_gen_scan.gen_cpu;
gzip_gen_slope = gzip_gen_scan.plain\gzip_gen_scan.gen_cpu;
lzo_gen_slope = lzo_gen_scan.plain\lzo_gen_scan.gen_cpu;

enc_scan_slope = enc_gen_scan.plain\enc_gen_scan.scan_cpu;
gzip_scan_slope = gzip_gen_scan.plain\gzip_gen_scan.scan_cpu;
lzo_scan_slope = lzo_gen_scan.plain\lzo_gen_scan.scan_cpu;

enc_sample=datasample(enc_gen_scan,300);
gzip_sample=datasample(gzip_gen_scan,300);
lzo_sample=datasample(lzo_gen_scan,300);

%{
hold on
plot(enc_gen_scan.plain, enc_gen_scan.plain*enc_gen_slope);
plot(gzip_gen_scan.plain, gzip_gen_scan.plain*gzip_gen_slope);
plot(lzo_gen_scan.plain, lzo_gen_scan.plain*lzo_gen_slope);

scatter(enc_sample.plain, enc_sample.gen_cpu, '*');
scatter(gzip_sample.plain, gzip_sample.gen_cpu, 'o');
scatter(lzo_sample.plain, lzo_sample.gen_cpu, '+');

legend('Best Encoding Fitting', 'GZip Fitting','LZO Fitting','Best Encoding','GZip','LZO');
%}
output = [enc_sample.plain enc_sample.gen_cpu enc_sample.scan_cpu gzip_sample.plain gzip_sample.gen_cpu gzip_sample.scan_cpu lzo_sample.plain, lzo_sample.gen_cpu lzo_sample.scan_cpu];

csvwrite('comp_time_plain_str.data',output);
