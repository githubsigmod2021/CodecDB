load('integer.mat')

train_data = f(1:1200,:);
train_label = label(1:1200,:);
test_data = f(1201:end,:);
test_label = label(1201:end,:);

tree = fitctree(train_data,train_label);
predict = predict(tree,test_data);

accuracy = sum(predict == test_label)/length(predict)