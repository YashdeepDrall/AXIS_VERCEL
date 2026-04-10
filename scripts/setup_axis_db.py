from app.db.mongodb import users_collection


def main():
    default_user = users_collection.find_one({})

    print("MongoDB setup is disabled in the current lightweight build.")
    print("The app now uses in-memory state for local runs.")

    if default_user:
        print(f"Default user ID: {default_user.get('userId')}")


if __name__ == "__main__":
    main()
