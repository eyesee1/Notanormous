connection = None

options = {
    
}

def setup_all(db):
    from notanormous.document import DOCUMENTS, Document
    Document._db = db
    
    # for cls in DOCUMENTS:
    #     cls._s(db)
    
    



    