minEnc = min(STRCOMPRESS{:,1:4},[],2);
minCom = min(STRCOMPRESS{:,5:7},[],2);
compare = STRCOMPRESS.minCom./STRCOMPRESS.minEnc;

cdfplot(minEnc);
hold on
h1=cdfplot(STRCOMPRESS.minCom);
h2=cdfplot(STRCOMPRESS.gz);
set(h1, 'LineStyle','--');
set(h2, 'LineStyle',':','Color','b');
cdfplot(STRCOMPRESS.lz);
cdfplot(STRCOMPRESS.sn);
title('');
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Selected Encoding', 'Best Compression', 'GZip','LZO','Snappy','Location','southeast');
hold off
saveas(gcf,'compress_str_cdf.eps','epsc');
close(gcf);

threshold=5;
compare(compare>threshold)=threshold;
h1=cdfplot(compare);
hold on
set(h1, 'LineStyle','--','color','r');
gzc = STRCOMPRESS.gz./STRCOMPRESS.minEnc;
gzc(gzc>threshold)=threshold;
h2=cdfplot(gzc);
set(h2, 'LineStyle',':','color','black');
lzc = STRCOMPRESS.lz./STRCOMPRESS.minEnc;
lzc(lzc>threshold)=threshold;
cdfplot(lzc);
snc = STRCOMPRESS.sn./STRCOMPRESS.minEnc;
snc(snc>threshold)=threshold;
cdfplot(snc);
title('');
xlabel('Compression / Encoding Ratio');
ylabel('Percentage');
legend('Best Compression', 'GZip','LZO','Snappy','Location','southeast');
saveas(gcf, 'compress_str_compare.eps','epsc');
close(gcf);