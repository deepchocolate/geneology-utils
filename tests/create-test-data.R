### Create some test data of a known pedigree structure
# To make it a bit more realistic, individual IDs are moved around a bit
library(pedtools)
N = 5
trios = lapply(1:N, function(i) nuclearPed() |> relabel((1:3) + 10*i))
# Add a son in the last trio
trios[[N]] <- addSon(trios[[N]], id=104, parents=founders(trios[[N]]))
# Replace the N-1 trio and use the son as a father
trios[[(N-1)]] <- nuclearPed(nch=2,father=104, mother=91, children=105:106)
df = do.call(rbind, lapply(trios, as.data.frame))
df$id <- as.integer(df$id)
df$mid <- as.integer(df$mid)
df$fid <- as.integer(df$fid)
df$sex <- as.integer(df$sex)
df$famid = rep(1:N, unlist(lapply(trios, function (x) length(x$ID))))
# Remove individuals with unknown parents ("founders") as this is likely how raw input looks like
# in registers.
df <- subset(df, !(fid == 0 & mid==0))
colnames(df) <- c('ID', 'FATHER', 'MOTHER', 'SEX','FAMID')

# Table with individual ID and family id
fams <- data.frame(
  FAMID=df$FAMID,
  ID=c(df$ID, df$MOTHER,df$FATHER)
)
fams <- unique(fams)

# Table with individual and parent id
offpar <- data.frame(
  ID = c(df$ID, df$ID),
  PID = c(df$FATHER, df$MOTHER)
)

### Write files
# Main input file: Individual ID - Mother ID - Father ID - Sex - Family ID
write.table(df, file='input/trios.tsv', sep="\t", quote=F, row.names=F)
# Family ID - Individual ID (Individuals recur for each family)
write.table(fams, file='input/family-membership.tsv', sep="\t", quote=F, row.names=F)
# Individual ID - Parent ID (Individuals recur for each parent)
write.table(offpar, file='input/offspring-parent.tsv', sep="\t", quote=F, row.names=F)
