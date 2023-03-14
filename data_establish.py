import csv
import py2neo
from py2neo import Graph, Node, Relationship, NodeMatcher

g = Graph('http://localhost:7474', user='neo4j', password='king001216',name='neo4j')


# 导入节点 电影类型
"""
with open('data/genre.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    for item in reader:
        if reader.line_num == 1:
            continue
        node = Node('Genre',gid = int(item[0]),gname = item[1])
        g.create(node)
"""

"""""
# 导入节点 演员信息	
with open('data/person.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    for item in reader:
        if reader.line_num == 1:
            continue
        node = Node('Person', pid=int(item[0]), birth=item[1], death=item[2],
                    name=item[3], biography=item[4], birthplace=item[5])
        g.create(node)




# 导入节点，电影信息
with open('data/movie.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    for item in reader:
        if reader.line_num == 1:
            continue
        print(item)
        node = Node('Movie', mid=int(item[0]), title=item[1], introduction=item[2],
                    rating=float(item[3]), releasedate=item[4])
        g.merge(node,"Movie","mid")



# 导入节点，电影参演者  1对多
with open('data/person_to_movie.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    matcher = NodeMatcher(g)
    for item in reader:
        if reader.line_num == 1:
            continue
        p = matcher.match("Person", pid=int(item[0]))
        m = matcher.match("Movie", mid=int(item[1]))
        if p is None or m is None:
            continue
        r = Relationship(p.first(),"actedin",m.first())
        print(r)
        g.merge(r)



with open('data/movie_to_genre.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    matcher = NodeMatcher(g)
    for item in reader:
        if reader.line_num == 1:
            continue
        m = matcher.match("Movie", mid=int(item[0]))
        G = matcher.match("Genre", gid=int(item[1]))
        print(m.first(),"-->",G.first())
        if m.first() is None or G.first() is None:
            continue
        r = Relationship(m.first(), "is", G.first())
        g.merge(r)

"""