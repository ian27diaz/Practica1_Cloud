import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer


# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db","labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("sqlite_images.db","images")

# Process Logic.
print("Ponme tus palabras separadas por espacios:")
words = input().split()

for word in words:
    stemmedWord = Stemmer.stem(word)
    i = 0
    while True:
        
        result = kv_labels.getSort(stemmedWord, str(i))
        #print("label result:",result, "with", i)
        if result == None:
            break
        imageResult = kv_images.get(result)
        #print("image result:", imageResult, "with", i)
        if(imageResult != None):
            print(result, "->", imageResult)
        i += 1
    #print("Going to next word from", word)

# Close KeyValues Storages
kv_labels.close()
kv_images.close()







