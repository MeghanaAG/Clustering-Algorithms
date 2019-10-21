# kmeans
my_kmeans <- function(dataset, n_cluster, initial_points) {
  if (nrow(dataset) < n_cluster) {
    stop('number of data rows must be bigger than number of cluster')
  }
  initial_points <- sample.int(nrow(dataset), size = n_cluster, replace = FALSE)
  centroids <- data.frame(dataset[initial_points,])
  
  max_iter = 500
  
  for (iter in 1:max_iter) {
    print(paste0('iter = ', iter))
    old_centroids <- centroids
    
    labels <- rep(-1, nrow(dataset))
    for (i in 1:nrow(dataset)) { # for each data point in dataset
      # find the centroids that has minimum distance to that data points
      distance <- rep(-1, n_cluster)
      for (j in 1:n_cluster) {
        # compute the distance to each centroid  
        distance[[j]] <- sqrt(sum((as.vector(centroids[j, ]) - as.vector(dataset[i,]))^2))
      }
      labels[[i]] <- which(distance == min(distance))
    }
    # update the centroids
    for (j in 1:n_cluster) {
      centroids[j, ] <- colMeans(dataset[which(labels == j),])
    }  
    
    if (norm(as.matrix(old_centroids) - as.matrix(centroids)) < 1e-10) {
      break;
    }
  }
  result <- list("labels"=labels, "centroids"=centroids)
}

# Hierarchical clustering with single linkage
my_hclust <- function(dist_mat) {
  dist_mat <- as.matrix(dist_mat)
  diag(dist_mat) <- Inf # set the diagonal to be infinity
  n <- nrow(dist_mat) # n is the number of objects
  
  group_membership <- -(1:n) # the vector that denotes group membership
  height <- rep(0, n-1) # vector that denote height of the branch
  merge <- matrix(0, nrow = n-1, ncol = 2) 
  # matrix that keeps track which objects are merged together 
  # do (n-1) merging steps
  for (i in 1:(n-1)) {
    # get the minimum distance in distance matrix
    height[i] = min(dist_mat)
    # get all the positions (row, column) in the matrix that has the smallest value
    indices <- which(dist_mat == min(dist_mat), arr.ind = TRUE)
    # the candidate pair is the first index (row, column)
    candidate_pair <- indices[1, ] # get the first index
    
    # write down the merge merge step
    merge[i, ] <- group_membership[sort(candidate_pair)]
    
    # form a new group
    group <- candidate_pair # new group consist of the candidate pair
    # if the first object of candidate pair belongs to a group, get all the objects of that group
    if (group_membership[candidate_pair[1]] > 0) {
      group <- c(group, which(group_membership %in% group_membership[candidate_pair[1]]))
    }
    # if the second object of candidate pair belongs to a group, get all the objects of that group
    if (group_membership[candidate_pair[2]] > 0) {
      group <- c(group, which(group_membership %in% group_membership[candidate_pair[2]]))
    }
    # revise the group membership
    group_membership[group] <- i
    
    # now, since we have merged the candidate pair into 1 group, the distance from that group 
    # to other data points will be the lesser distance among the two distances
    new_distance_from_candidate_pair <- apply(dist_mat[candidate_pair, ], 2, min)
    
    # update the distance matrix
    dist_mat[candidate_pair[1], ] <- new_distance_from_candidate_pair
    dist_mat[, candidate_pair[1]] <- new_distance_from_candidate_pair
    dist_mat[candidate_pair[1], candidate_pair[1]] <- Inf
    dist_mat[candidate_pair[2], ] <- Inf
    dist_mat[, candidate_pair[2]] <- Inf
    
  }
  #list(merge = merge, height = height, labels = rownames(dist_mat))
  structure(list(merge = merge, height = height, 
                 labels = rownames(dist_mat), method = "single", 
                 dist.method = "euclidean",
                 order = my_order(merge)), 
            class = "hclust")
}

#function to compute jaccard coefficient
jaccard_and_rand <- function(truelabel,predictedlabel){
  pmatrix<-matrix( ,nrow=nrow(dataset),ncol=nrow(dataset))
  cmatrix<-matrix( ,nrow=nrow(dataset),ncol=nrow(dataset))
  
  for(i in 1:nrow(dataset)){
    for(j in 1:nrow(dataset)){
      if(truelabel[i]==truelabel[j]){
        pmatrix[i,j]=1
      }
      else{
        pmatrix[i,j]=0
      }
    }
  }
  
  for(i in 1:nrow(dataset)){
    for(j in 1:nrow(dataset)){
      if(predictedlabel[i]==predictedlabel[j]){
        cmatrix[i,j]=1
      }
      else{
        cmatrix[i,j]=0
      }
    }
  }
  
  m00 = 0
  m11 = 0
  m01 = 0
  m10 = 0
  
  for(i in 1:nrow(pmatrix)){
    for(j in 1:nrow(pmatrix)){
      if(cmatrix[i,j]==1 & pmatrix[i,j]==1){
        m11 = m11+1
      }
      else if (cmatrix[i,j]==0 & pmatrix[i,j]==0){
        m00 = m00 + 1
      }
      else if (cmatrix[i,j]==1 & pmatrix[i,j]==0){
        m10 = m10 + 1
      }
      else {
        m10 = m10 + 1
      }
    }
  }
  
  jaccard_coefficient=m11/(m11+m10+m01)
  
  print('Jaccard Co-efficient is')
  
  print(jaccard_coefficient)
  
  print('Rand Index is')
  
  rand_index=(m11+m00)/(m11+m00+m10+m01)
  
  print(rand_index)
  #c(jaccard_coefficient, rand_index)
}


#spectral clustering algorithm
spectral <- function (dataset,n_cluster){
  similarity_matrix <- matrix(,nrow=nrow(dataset),ncol=nrow(dataset))
  sigma=1.7
  
  #computing the entries of similarity matrix
  for(i in 1:nrow(similarity_matrix)){
    for(j in 1:ncol(similarity_matrix)){
      if(i==j){
        similarity_matrix[i,j]=0
      }
      else{
        similarity_matrix[i,j]=exp(-((sqrt(sum((dataset[i,]-dataset[j,])^2)))^2)/(sigma^2))
      }
    }
  }
  
  degree_matrix <-  diag(,nrow=nrow(dataset),ncol=nrow(dataset))
  
  #computing the values of degree matrix
  for(i in 1:nrow(degree_matrix)){
    degree_matrix[i,i] <- sum(similarity_matrix[i,])
  }
  
  #computing the Laplacian 
  
  laplacian_matrix <-  matrix(,nrow=nrow(dataset),ncol=nrow(dataset))
  for(i in 1:nrow(laplacian_matrix))
    laplacian_matrix[i,] <- degree_matrix[i,] - similarity_matrix[i,]
  
  #
  
  
  eigen_decomposition <- eigen(laplacian_matrix)
  eigen_values <- eigen_decomposition$values
  eigen_vectors <- eigen_decomposition$vectors
  
  plot_df <- data.frame(eigval=eigen_values,eigvec=eigen_vectors,data_1_d=pca$x[,1])
  
  #finding the largest difference in eigen values
  maximum_diff = eigen_values[2]-eigen_values[1]
  eigen_val_i_index=1
  eigen_val_i1_index=2
  for(i in 2:nrow(plot_df)){
    if(i+1 <= nrow(plot_df)){
      if(eigen_values[i+1]-eigen_values[i] > maximum_diff){
        maximum_diff = eigen_values[i+1]-eigen_values[i]
        eigen_val_i_index=i
        eigen_val_i1_index=(i+1)
      }
    }
  }
  
  #the largest difference in eigen values is between eigen_val_i_index and eigen_val_i1_index
  #now we need to select eigenvectors 1 to eigen_val_i_index
  #and then perform k-means on the eigen vectors
  number_of_col=ncol(plot_df[,3:eigen_val_i_index])
  embedded_space_matrix <- matrix(,nrow=nrow(plot_df),ncol=number_of_col)
  
  embedded_space_matrix <- plot_df[,3:eigen_val_i_index]
  
  #performing k means on embedded space matrix
  
  spec_my_km <- my_kmeans(embedded_space_matrix, n_cluster)
  #spec_my_km <- kmeans(embedded_space_matrix, n_cluster)
}

# this function is used when plotting the dendrogram
# this function reorders the nodes in a particular order so that 
# there is no crossing in dendrogram
my_order = function(merge)
{
  n_datapoints = nrow(merge) + 1
  result = rep(0,n_datapoints)
  # the first two nodes that are children of the root
  result[1] = merge[n_datapoints-1,1]
  result[2] = merge[n_datapoints-1,2]
  position = 2 
  for(i in seq(n_datapoints-2,1))
  {
    for(j in seq(1,position))
    {
      if(result[j] == i)
      {
        result[j] = merge[i,1]
        if(j==position) {
          position = position + 1
          result[position] = merge[i,2]
        } 
        else {
          position = position + 1
          for(k in seq(position, j+2)) {
            result[k] = result[k-1]
          }
          result[j+1] = merge[i,2]
        }
        
      }
    }
  }
  -result
}

my_dbscan <- function(dataset,epsilon,minpts){
  eps = epsilon
  #creating a distance matrix of data points
  d_matrix <- as.matrix(dist(dataset, method = "euclidean"))
  print("average distance")
  print(mean(d_matrix))
  #print("minimum distance")
  #print(min(d_matrix))
  
  visited <- as.list(rep(0,nrow(dataset)))
  clusters <- as.list(rep(0,nrow(dataset)))
  next_cluster_num = 0
  
  #region query function
  region_query <- function(point,eps){
    neighbors <- list()
    for(i in 1:ncol(d_matrix)){
      
      if(d_matrix[point,i] <= eps ){
        neighbors <- as.list(c(neighbors,i))
      }
      
    }
    return(neighbors)
  }
  
  expandCluster <- function(point,neighborPts,eps,minpts,env=parent.frame()){
    #add point P to cluster C
    env$clusters[[point]] = next_cluster_num
    
    x=0
    n=1
    while(x==0){
      
      
      if(env$visited[[neighborPts[[n]]]]==0){
        env$visited[[neighborPts[[n]]]] = 1
        neighbors_of_neighbor_n <- region_query(neighborPts[[n]],eps)
        if(length(neighbors_of_neighbor_n) >= minpts){
          
          neighborPts <- as.list(unique(c(neighborPts,neighbors_of_neighbor_n)))
          
        }
      }
      
      if(env$clusters[[neighborPts[[n]]]]==0){
        env$clusters[[neighborPts[[n]]]]=next_cluster_num
        
      }
      
      if(n==length(neighborPts)){
        x=1
      }
      
      
      n = n+1
      
    }
  }
  
  #algorithm starts here
  #print(d_matrix)
  for(pt in 1:nrow(dataset)){
    if(visited[[pt]]==0){
      visited[[pt]] = 1
      neighbors <- region_query(pt,eps)
      if(length(neighbors) < minpts){
        clusters[[pt]] = 0
      }
      else{
        next_cluster_num = next_cluster_num +1 
        expandCluster(pt,neighbors,eps,minpts)
      }
    }
  }
  
  final_clusters<-as.list(clusters)
  
}