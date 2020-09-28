clear;
load('compress_enc.mat');

int_gz = caeint.gz./caeint.enc;
int_lz = caeint.lz./caeint.enc;
int_snappy = caeint.snappy./caeint.enc;

cdfplot(int_gz);
hold on
cdfplot(int_lz);
cdfplot(int_snappy);
hold off
legend('GZip', 'LZO','Snappy','Location','southeast');
title('');
xlabel('Compression Ratio on Encoded Data');
ylabel('Percentage');
saveas(gcf,'compress_enc_int.eps','epsc');
close(gcf);

str_gz = caestr.gz./caestr.enc;
str_lz = caestr.lz./caestr.enc;
str_snappy = caestr.snappy./caestr.enc;

cdfplot(str_gz);
hold on
cdfplot(str_lz);
cdfplot(str_snappy);
hold off
legend('GZip', 'LZO','Snappy','Location','southeast');
title('');
xlabel('Compression Ratio on Encoded Data');
ylabel('Percentage');
saveas(gcf,'compress_enc_str.eps','epsc');
close(gcf);


dbl_gz = caedob.gz./caedob.enc;
dbl_lz = caedob.lz./caedob.enc;
dbl_snappy = caedob.snappy./caedob.enc;

cdfplot(dbl_gz);
hold on
cdfplot(dbl_lz);
cdfplot(dbl_snappy);
hold off
legend('GZip', 'LZO','Snappy','Location','southeast');
title('');
xlabel('Compression Ratio on Encoded Data');
ylabel('Percentage');
saveas(gcf,'compress_enc_dbl.eps','epsc');
close(gcf);