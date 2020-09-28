clear;
load('compress_data.mat');

minEnc = min(INT{:,1:5},[],2);
minCom = min(INT{:,6:8},[],2);

edges = [0,0.25,0.5,0.75,1,inf];

datas = {minEnc, minCom, INT.lz, INT.sn};
bins = {};

for i = 1:4
    bins{i} = discretize(datas{i}, edges);
end

datacell = [];
grouping = [];
widths =[];

for i = 1:5
    for j = 1:4
        index = i*4+j;
        data = datas{j};
        bin_data = data(bins{j}==i);
        datacell = [datacell bin_data'];
        grouping = [grouping ones(length(bin_data),1)'.*index-1];
        widths = cat(1,widths, length(bin_data));
    end
end

widths(widths==0)=1;

widths=widths./max(widths);

boxplot(datacell,grouping,'Widths',widths);

    