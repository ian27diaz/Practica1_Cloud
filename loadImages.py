import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTriple
import keyvalue.stemmer as Stemmer
from dynamo import dynamostorage as Dynamo
# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("sqlite_images.db","images")
#Make connections to Dynamo storage
dynamo = Dynamo.DynamoStorage("C:\\Users\\IanGod\\Documents\\ITESO\\8vo Semestre\\Computo en la nube\\Practica-1\\dynamo\\config.json")

# Process Logic.
parserImages = ParseTriple.ParseTriples("images.ttl")
parserLabels = ParseTriple.ParseTriples("labels_en.ttl")
############### Poblando sqlite_images.db ###############
images = []
for img in images:
    print("img => ", img)
contadorImagenes = 0
while contadorImagenes < 50:
    tupla = parserImages.getNext()
    if tupla != None and tupla[1] == "http://xmlns.com/foaf/0.1/depiction":
        #Le quito <B> de -> <A><B><C>:
        tupla = tupla[:1] + tupla[2:]
        images.append(tupla)
        #print("Inserted in images:",tupla[0], "->", tupla[1])
        kv_images.put(tupla[0], tupla[1])
        dynamo.putImage(tupla[0], tupla[1])
        contadorImagenes += 1
############### Poblando sqlite_labels.db ###############
#print(images)
#print("NOW FROM LABELS ******************************************")
etiquetas = []
stemmedWords = []
stemmedWordsIndexes = []
contadorLabels = 0
while contadorLabels < 100:
    tupla = parserLabels.getNext()
    if(tupla == None) :
        break
    if(tupla[1] == "http://www.w3.org/2000/01/rdf-schema#label"):
        tupla = tupla[:1] + tupla[2:]
        #print(tupla)
        for img in images:
            if img[0] == tupla[0]:
                #Si existe la imagen en la DB.
                words = tupla[1].split()
                for word in words:
                    stemmedWord = Stemmer.stem(word)
                    i = 0
                    wordFound = False
                    while i < len(stemmedWords):
                        if(stemmedWords[i] == stemmedWord):
                            stemmedWordsIndexes[i] += 1
                            kv_labels.putSort(stemmedWord, str(stemmedWordsIndexes[i]), tupla[0])
                            dynamo.putLabel(stemmedWord, str(stemmedWordsIndexes[i]), tupla[0])
                            wordFound = True
                            #print("Inserted(rep)", stemmedWord, "->", tupla[0])
                            break
                        i = i + 1
                    if(wordFound == False):
                        stemmedWords.append(stemmedWord)
                        stemmedWordsIndexes.append(0)
                        kv_labels.putSort(stemmedWord, "0", tupla[0])
                        dynamo.putLabel(stemmedWord, "0", tupla[0])
                        #print("Inserted", stemmedWord, "->", tupla[0])
                break
        etiquetas.append(tupla)
        contadorLabels += 1

print("Label processing done")
# Close KeyValues Storages
kv_labels.close()
kv_images.close()







