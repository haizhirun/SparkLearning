import os
import sys
from scipy.sparse.csr import csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from operator import itemgetter

path_rating = "D:\\ratings.txt"
path_itemtext = "D:\\document.txt"
min_rating = 3
_max_length = 4
_max_df = 0.7
_vocab_size = 100

if os.path.isfile(path_rating):
    raw_ratings = open(path_rating, 'r')
    print("Path - rating data: %s" % path_rating)
else:
    print("Path(rating) is wrong!")
    sys.exit()
if os.path.isfile(path_itemtext):
    raw_content = open(path_itemtext, 'r')
    print("Path - document data: %s" % path_itemtext)
else:
    print("Path(item text) is wrong!")
    sys.exit()

# 第一次扫描文本文件 过滤出有文档的项目
### list是可重复集合；set是不可重复集合
tmp_id_plot = set() # 初始化项目id集合
### splitlines() 按照行分隔，返回一个包含各行作为元素的列表
all_line = raw_content.read().splitlines()
for line in all_line:
    tmp = line.split('::')
    i = tmp[0] # 项目编号
    tmp_plot = tmp[1].split('|')
    if tmp_plot[0] == '': # 若第一项文档内容为空（即没有文档项目），则进行下一次for循环
        continue
    tmp_id_plot.add(i) # 将有文档内容的项目id添加到集合中
# tmp_id_plot_l = list(tmp_id_plot)
# print("tmp_id_plot:\n %s" % tmp_id_plot)
raw_content.close()

print("Preprocessing rating data...")
print("\tCounting # ratings of each user and removing users having less than %d ratings..." % min_rating)
# 第一次扫描评分文件，统计每个用户的评分数
all_line = raw_ratings.read().splitlines()
tmp_user = {}
for line in all_line:
    tmp = line.split('::')
    u = tmp[0]  # 用户编号
    i = tmp[1]  # 项目编号
    if (i in tmp_id_plot):
        if (u not in tmp_user):
            tmp_user[u] = 1
        else:
            tmp_user[u] = tmp_user[u] + 1 # 计算每个用户的评分数
# print("tmp_user:\n %s" % tmp_user)
raw_ratings.close()

# 第二次扫描评分文件，去除不满足给定条件的用户和项目，形成用户和项目的矩阵索引
raw_ratings = open(path_rating, 'r')
all_line = raw_ratings.read().splitlines()
userset = {}
itemset = {}
user_idx = 0
item_idx = 0
user = []
item = []
rating = []
for line in all_line:
    tmp = line.split('::')
    u = tmp[0]
    if u not in tmp_user:
        continue
    i = tmp[1]
    # 当用户的评分数目小于最小评分数目min_rating时，该用户将被跳过
    if tmp_user[u] >= min_rating:
        if u not in userset:
            userset[u] = user_idx
            user_idx = user_idx + 1
        if (i not in itemset) and (i in tmp_id_plot): # 项目有文本内容
            itemset[i] = item_idx
            item_idx = item_idx + 1
    else:
        continue
    if u in userset and i in itemset:
        u_idx = userset[u]
        i_idx = itemset[i]
        user.append(u_idx)
        item.append(i_idx)
        rating.append(float(tmp[2]))
raw_ratings.close()
R = csr_matrix((rating, (user, item)))
# print(R.shape[1])
print("userset: %s\nitemset: %s" % (userset,itemset) )
# print("user: %s\nitem: %s" % (user,item) )
print("Finish preprocessing rating data - # user: %d, # item: %d, # ratings: %d" % (R.shape[0], R.shape[1], R.nnz))
# 第二次扫描文本文件，根据项目在评分矩阵中的索引形成idx2plot字典
print("Preprocessing item document...")
# Read Document File
raw_content = open(path_itemtext, 'r')
max_length = _max_length # 每个项目的文本的最大长度
map_idtoplot = {} # ？？？
all_line = raw_content.read().splitlines()
for line in all_line:
    tmp = line.split('::')
    if tmp[0] in itemset:
        i = itemset[tmp[0]] # 项目tmp[0]第一次出现时的id
        tmp_plot = tmp[1].split('|')
        eachid_plot = (' '.join(tmp_plot)).split()[:max_length] # 保留每个项目的文本的长度为max_length
        map_idtoplot[i] = ' '.join(eachid_plot)
print("map_idtoplot: \n%s" % map_idtoplot)
print("\tRemoving stop words...")
# print("\tFiltering words by TF-IDF score with max_df: %.1f, vocab_size: %d" % (_max_df, _vocab_size))
# 通过文档形成字典
'''
TfidfVectorizer
    stop_words:如果为english，用于英语内建的停用词列表
    max_df：当构建词汇表时，严格忽略高于给出阈值的文档频率的词条
    max_features：构建一个词汇表，仅考虑max_features–按语料词频排序
'''
vectorizer = TfidfVectorizer(max_df=_max_df, stop_words={'english'}, max_features=_vocab_size)
Raw_X = [map_idtoplot[i] for i in range(R.shape[1])]
print("Raw_X: \n%s" % Raw_X)
vectorizer.fit(Raw_X)
vocab = vectorizer.vocabulary_
print("vocab: \n%s" % vocab)
# print(list(vocab.items()))
X_vocab = sorted(list(vocab.items()), key=itemgetter(1))
print("X_vocab: \n%s" % X_vocab)
# Make input for run
X_sequence = []
for i in range(R.shape[1]):

    X_sequence.append(
        [vocab[word] + 1 for word in map_idtoplot[i].split() if word in vocab])
'''Make input for CTR & CDL'''
baseline_vectorizer = CountVectorizer(vocabulary=vocab)
# X_base = baseline_vectorizer.fit_transform(Raw_X).toarray()
X_base = baseline_vectorizer.fit_transform(Raw_X)
D_all = {
    'X_sequence': X_sequence,
    'X_base': X_base,
    'X_vocab': X_vocab,
}
print("Finish preprocessing document data!")
# print("R: \n%s" % R)
print("D_all: \n%s" % D_all)
