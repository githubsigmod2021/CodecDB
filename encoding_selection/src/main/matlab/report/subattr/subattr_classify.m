clear;
load('subattr_classify.mat');

SUBATTRCLASSIFY.value5(SUBATTRCLASSIFY.value5 <= 0.99) = 0;
SUBATTRCLASSIFY.value5(SUBATTRCLASSIFY.value5 > 0.99) = 1;

[trainedModel, accuracy] = trainClassifier(SUBATTRCLASSIFY);

save("classify_model.mat",'trainedModel');