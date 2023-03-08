import PRZipDownloader
import PdddmmyyReader
import pd_move_to_model
import nameChangeModel
import corpAction


def main():
    # download PR files
    PRZipDownloader.update()
    # update pd file data
    PdddmmyyReader.update()
    pd_move_to_model.update()
    # update corp actions
    corpAction.update()
    # update name change
    nameChangeModel.update()


if __name__ == "__main__":
    main()
