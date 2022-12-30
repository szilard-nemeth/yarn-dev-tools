from yarndevtools.commands_common import GSheetArguments, ArgumentParserUtils, MongoArguments


class UnitTestResultAggregatorDatabaseParserParams:
    @staticmethod
    def add_params(parser):
        MongoArguments.add_mongo_arguments(parser)
