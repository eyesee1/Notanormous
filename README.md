# Notanormous

Notanormous is completely and utterly not an ORM. It sort of rhymes with 'enormous,' which is kinda like 'humongous', which is where the 'mongo' in MongoDB comes from, which is wildly appropriate because Notanormous is meant to be used with MongoDB and no other DB, because they are no fun anymore.

## Probably Don't Use This!

Last time I checked, this is still in production at my former employer, where it is unlikely to ever get hit by more than 12 users at once. It seems to be working fine, however I would not consider this battle-tested. It's not memory efficient, which is fine for a small company's internal server. I'd look into other options before throwing lots of users at it. Aside from that, have fun with it.

I'm mainly publishing this so that prospective clients or employers can see that I know how to use advanced Python concepts like Metaclass programming. Check `document.py` to see that part.

## The Run-Down

* The best way to see how this works is to read `tests/tests.py`
* If you prefer trying to make MongoDB act like a relational database because that is the hammer you prefer to hit things with, this is the wrong project for you. (Or maybe it's the right one if you're trying to stop doing that.)
* It has the flexibility you'd expect when using a 'NoSQL' style database such as MongoDB. ORMs tend to define a very rigid structure which is difficult and error-prone to change.
* It has some minimal features you may expect from an ORM, like some simple validation and required fields. It doesn't try to make MongoDB act like a relational database, so it does not define any relationships like an ORM would except for using `ObjectId` and `DBRef` similarly to how foreign keys work.
* Some limited document-saving conveniences, such as stored properties.
* No inheritance, but you can embed documents in other documents (or even lists of documents) and designate any document class as embed-only.
* No Session management or Unit of Work concept. So be careful. You might accidentally have multiple copies of the same document in memory at once and they will not know about each other. Be extra careful when following references like `person.books`. If you access that data another way, you could create conflicts. Mindfulness is key. Clean up after yourself or suffer the consequences. Your app, not Notanormous is responsible for preventing conflicts, such as two users editing the same document at once, and then one of them wipes out the other's changes (the last to save wins).
* For the strictly-defined parts of a document, you use attribute (dot) style access, e.g., `my_car.is_orange`. For the flexible, nebulous bits, you use dict-style access (as you would with pymongo directly), e.g., `my_car["something_not_every_car_has"]`. Some people may find this awkward or confusing, and you certainly *can* shoot yourself in the foot with it. It's up to you to know what you're doing here.
* Like Unix, Notanormous assumes you know what you're doing. If that makes you nervous, this is not the project for you.
* Currently, this is fairly poorly documented, but you can look at the tests for some examples of how things work.
* Hopefully you didn't already have a Mongo database called `notanormous_tests` when you ran nosetests. If you did, whoops, sorry. (BTW, the tests currently assume you have a `mongod` running on localhost which doesn't require any auth. That may be fixed one day.)

