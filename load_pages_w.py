import json


def hashmap_pagerank():
    hmap_pr = {}

    #archivo de page ranking diccionario
    f = open('wiki/result/part-r-00000', 'r')

    for line in f:
        row = line.split('\t')
        pageDoc = row[1][:-1].strip()
        ranking = row[0].rstrip("\n")
        hmap_pr[pageDoc] = ranking
        #print(row[1] + "->" + hmap_pr[row[1]])
    return hmap_pr


def hashmap():
    words = {}

    f = open('wiki/inverted/part-r-00000', 'r')
    
    for line in f:
        #row[0] -> "Keyword:$"
        #row[1] -> "IndexResult:{page=count,...}"
        row = line.split(',"Index')
        #quitando comillas dejando la keyword sola
        keyword = row[0].split(":")[1][1:-1]
        #quitando llaves y comillas dejando la lista de paginas
        listaPages = row[1].split(":")[1][2:-4]
        words[keyword] = [page[:-2].strip() for page in listaPages.split(",")]
        #    words[row[0]] = [w.split(':')[0] for w in row[1].split(',')]

    return words

inverted_index = hashmap()	

def search_query(value):

    words = []
    
    #ojo haz un print de lo quq recibes para el backend
    for word in value.split(" "):
        words.append(word)
        #una normalizacion pero q tambien debe estar en los resultados del invertido y ranking
        #lo cual no esta asi que lo obviamos
        
        #word = porter.stem(word, 0, len(word)-1)
    if len(value):		
        result = []
        i = 0
        for word in value.split(" "):
            if word in inverted_index:
                result.append([inverted_index[word], words[i]])
                i += 1

        return result

    return []			
