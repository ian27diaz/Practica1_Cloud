import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
import dynamo.dynamostorage as DynamoStorage

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("sqlite_images.db","images")
# Make connections to Dynamo
dynamo = DynamoStorage.DynamoStorage("C:\\Users\\IanGod\\Documents\\ITESO\\8vo Semestre\\Computo en la nube\\Practica-1\\dynamo\\config.json")
# Process Logic.
print("Ponme tus palabras separadas por espacios:")
words = input().split()
#Results from dynamo:
dynamoResults = {}

# for word in words:
#     stemmedWord = Stemmer.stem(word)
#     i = 0
#     while True:
        
#         result = kv_labels.getSort(stemmedWord, str(i))
#         print("label result:",result, "with", i)
#         if result == None:
#             break
#         imageResult = kv_images.get(result)
#         #print("image result:", imageResult, "with", i)
#         if(imageResult != None):
#             print(stemmedWord, "->", imageResult)
#         i += 1
#     #print("Going to next word from", word)

#test of dynamostorage
for word in words:
    stemmedWord = Stemmer.stem(word)
    dynamoResults[stemmedWord] = dynamo.queryImageByLabel(stemmedWord)

print(dynamoResults)

# Close KeyValues Storages
kv_labels.close()
kv_images.close()







