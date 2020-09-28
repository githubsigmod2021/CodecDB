minEnc = min(DOUBLECOMPRESS{:,1:2},[],2);
minCom = min(DOUBLECOMPRESS{:,3:5},[],2);
compare = minCom./minEnc;

cdfplot(minEnc);
hold on
h1=cdfplot(minCom);
h2=cdfplot(DOUBLECOMPRESS.gz);
set(h1, 'LineStyle','--','Color','blue');
set(h2, 'LineStyle',':');
cdfplot(DOUBLECOMPRESS.lz);
cdfplot(DOUBLECOMPRESS.sn);
title('');
xlabel('Compression Ratio');
ylabel('Percentage');
legend('Selected Encoding', 'Best Compression', 'GZip','LZO','Snappy','Location','southeast');
hold off
saveas(gcf,'compress_dbl_cdf.eps','epsc');
close(gcf);


threshold=5;
compare(compare>threshold)=threshold;
h1=cdfplot(compare);
hold on
set(h1, 'LineStyle','--','color','r');
gzc = DOUBLECOMPRESS.gz./minEnc;
gzc(gzc>threshold)=threshold;
h2=cdfplot(gzc);
set(h2, 'LineStyle',':','color','black');
lzc = DOUBLECOMPRESS.lz./minEnc;
lzc(lzc>threshold)=threshold;
cdfplot(lzc);
snc = DOUBLECOMPRESS.sn./minEnc;
snc(snc>threshold)=threshold;
cdfplot(snc);
title('');
xlabel('Compression / Encoding Ratio');
ylabel('Percentage');
legend('Best Compression', 'GZip','LZO','Snappy','Location','southeast');
saveas(gcf, 'compress_dbl_compare.eps','epsc');
close(gcf);