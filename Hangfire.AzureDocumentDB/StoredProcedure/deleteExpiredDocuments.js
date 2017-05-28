/**
 * Expiration manager to delete old expired documents
 * @param {number} type - The type of the document to delete
 * @param {number} expiryDate - The expiry unix time
 * @returns {number} number of documents deleted 
 */
function deleteExpiredDocuments(type, expiryDate) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) {
        if (type === 4 && doc.type === type && doc.counter_type === 2)
            return false;
        return doc.type === type && doc.expire_on <= expiryDate;
    }, function (err, documents) {
        if (err) throw err;

        if (documents.length > 0) {
            for (var i = 0; i < documents.length - 1; i++) {
                var self = documents[i]._self;
                collection.deleteDocument(self);
            }
        }

        response.setBody(documents.length);
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}