setwd("~/study/CSE601/project/project2")
source('utility.R')
library(ggplot2)
library(dplyr)
library(sparcl)

dataset_name = "iyer"
#load the dataset
dataset = read.csv(file = paste0(dataset_name, ".txt"), header = FALSE, sep = "\t")
colnames(dataset) <- c("id", "cluster", paste0("attr", 1:(ncol(dataset)- 2)))
n_cluster <- length(unique(dataset[dataset$cluster > 0, "cluster"]))
dataset$cluster <- as.factor(dataset$cluster)

# remove column that have zero variance
delete_col <- which(apply(dataset[3:ncol(dataset)], 2, var) < 1e-16)
if (length(delete_col) > 0) {
  dataset <- dataset[, -(2+delete_col)]  
}

######## PCA #######################################
pca <- prcomp(dataset[, 3:ncol(dataset)], scale. = TRUE, center = TRUE)
df <- data.frame(x=pca$x[, 1], y=pca$x[, 2], cluster=dataset$cluster)
ggplot(data = df, aes(x=x, y=y, color=cluster)) + geom_point() +
  ggsave(filename = paste0("PCA_", dataset_name, ".pdf"))
####### Hierarchical Clutering with Single Linkage #
print("Hierarchical Clutering with Single Linkage")
distance_matrix <- dist(dataset[, 3:ncol(dataset)], diag = TRUE, upper = TRUE, 
                        method = "euclidean")
ptm <- proc.time()
hc <- my_hclust(distance_matrix)
proc.time() - ptm
clusterCut2 <- cutree(hc, n_cluster)
df <- data.frame(x=pca$x[, 1], y=pca$x[, 2], cluster=as.factor(clusterCut2))
ggplot(data = df, aes(x=x, y=y, color=cluster)) + geom_point() +
  ggsave(filename = paste0("PCA_hc_", dataset_name, ".pdf"))
if (nrow(dataset) < 100) {
  pdf(paste0('dendrogram_', dataset_name, '.pdf'))
} else {
  pdf(paste0('dendrogram_', dataset_name, '.pdf'), width = 100)
}
plot(hc)
dev.off()
# jaccard coefficient and rand index
jaccard_and_rand(dataset$cluster, clusterCut2)
####### Kmeans ######################################
print("Kmeans")
initial_points = 1:n_cluster
ptm <- proc.time()
my_km <- my_kmeans(dataset[, 3:ncol(dataset)], n_cluster, initial_points)
proc.time() - ptm
df <- data.frame(x=pca$x[, 1], y=pca$x[, 2], cluster=as.factor(my_km$labels))
ggplot(data = df, aes(x=x, y=y, color=cluster)) + geom_point() +
  ggsave(filename = paste0("PCA_kmeans_", dataset_name, ".pdf"))

# jaccard coefficient and rand index
jaccard_and_rand(dataset$cluster, my_km$labels)

####### Kmeans in MapReduce #########################
print("Kmeans in MapReduce")
cluster_result <- read.csv(file = "part-r-00000", header = FALSE, sep = "\t")
colnames(cluster_result) <- c("row_index", "cluster")
cluster_result$cluster <- cluster_result$cluster + 1
cluster_result <- arrange(cluster_result, row_index)

df <- data.frame(x=pca$x[, 1], y=pca$x[, 2], cluster=as.factor(cluster_result$cluster))
ggplot(data = df, aes(x=x, y=y, color=cluster)) + geom_point() +
  ggsave(filename = paste0("PCA_kmeansMR_", dataset_name, ".pdf"))

# # jaccard coefficient and rand index
jaccard_and_rand(dataset$cluster, my_km$labels)
####### DBScan #########################
print("DBScan")
our_db <- my_dbscan(as.matrix(dataset[, 3:ncol(dataset)]),eps=3.3,minpts=45)
df <- data.frame(x=pca$x[, 1], y=pca$x[, 2], cluster=as.factor(unlist(our_db)))
ggplot(data = df, aes(x=x, y=y, color=cluster)) + geom_point()
# jaccard coefficient and rand index
jaccard_and_rand(dataset$cluster, unlist(our_db))
