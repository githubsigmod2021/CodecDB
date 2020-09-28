clear;
load('compress_data.mat');

minEnc = min(INT{:,1:5},[],2);
minCom = min(INT{:,6:8},[],2);
compare = minCom./minEnc;

cdfplot(minEnc);
hold on
h2=cdfplot(minCom);
set(h2, 'LineStyle','--','color','r');
h3=cdfplot(INT.gz);
set(h3, 'LineStyle',':','color','b');
cdfplot(INT.lz);
cdfplot(INT.sn);
title('');
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Selected Encoding', 'Best Compression', 'GZip','LZO','Snappy','Location','southeast');
hold off
saveas(gcf,'compress_int_cdf.eps','epsc');
close(gcf);

threshold = 5;

bc=compare;
bc(bc>threshold)=threshold;
h1=cdfplot(bc);
set(h1, 'LineStyle','--','color','r');
hold on
gzc = INT.gz./minEnc;
gzc(gzc>threshold)=threshold;
h2=cdfplot(gzc);
set(h2, 'LineStyle',':','color','b');
lzc = INT.lz./minEnc;
lzc(lzc>threshold)=threshold;
cdfplot(lzc);
snc = INT.sn./minEnc;
snc(snc>threshold)=threshold;
cdfplot(snc);
title('');
xlabel('Compression / Encoding Ratio');
ylabel('Percentage');
legend('Best Compression', 'GZip','LZO','Snappy','Location','southeast');
saveas(gcf, 'compress_int_compare.eps','epsc');
close(gcf);