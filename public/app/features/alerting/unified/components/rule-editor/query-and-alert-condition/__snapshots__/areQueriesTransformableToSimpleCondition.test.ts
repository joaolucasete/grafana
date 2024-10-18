// QueryAndExpressionsStep.test.tsx

import { produce } from 'immer';

import { EvalFunction } from 'app/features/alerting/state/alertDef';
import { ExpressionQuery, ExpressionQueryType, ReducerMode } from 'app/features/expressions/types';
import { AlertDataQuery, AlertQuery } from 'app/types/unified-alerting-dto';

import { areQueriesTransformableToSimpleCondition } from '../QueryAndExpressionsStep';
import { SimpleConditionIdentifier } from '../SimpleCondition';

const dataQuery: AlertQuery<AlertDataQuery | ExpressionQuery> = {
  refId: SimpleConditionIdentifier.queryId,
  datasourceUid: 'abc123',
  queryType: '',
  model: { refId: SimpleConditionIdentifier.queryId },
};

const reduceExpression: AlertQuery<ExpressionQuery> = {
  refId: SimpleConditionIdentifier.reducerId,
  queryType: 'expression',
  datasourceUid: '__expr__',
  model: {
    type: ExpressionQueryType.reduce,
    refId: SimpleConditionIdentifier.reducerId,
    settings: { mode: ReducerMode.Strict },
  },
};
const thresholdExpression: AlertQuery<ExpressionQuery> = {
  refId: SimpleConditionIdentifier.thresholdId,
  queryType: 'expression',
  datasourceUid: '__expr__',
  model: {
    type: ExpressionQueryType.threshold,
    refId: SimpleConditionIdentifier.thresholdId,
  },
};

const expressionQueries: Array<AlertQuery<ExpressionQuery>> = [reduceExpression, thresholdExpression];

describe('areQueriesTransformableToSimpleCondition', () => {
  it('should return false if dataQueries length is not 1', () => {
    // zero dataQueries
    expect(areQueriesTransformableToSimpleCondition([], expressionQueries)).toBe(false);
    // more than one dataQueries
    expect(areQueriesTransformableToSimpleCondition([dataQuery, dataQuery], expressionQueries)).toBe(false);
  });
  it('should return false if expressionQueries length is not 2', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [dataQuery];
    const result = areQueriesTransformableToSimpleCondition(dataQueries, []);
    expect(result).toBe(false);
  });

  it('should return false if the dataQuery refId does not match SimpleConditionIdentifier.queryId', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [
      { refId: 'notSimpleCondition', datasourceUid: 'abc123', queryType: '', model: { refId: 'notSimpleCondition' } },
    ];
    const result = areQueriesTransformableToSimpleCondition(dataQueries, expressionQueries);
    expect(result).toBe(false);
  });
  it('should return false if no reduce expression is found with correct type and refId', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [dataQuery];
    const result = areQueriesTransformableToSimpleCondition(dataQueries, [
      { ...reduceExpression, refId: 'hello' },
      thresholdExpression,
    ]);
    expect(result).toBe(false);
  });

  it('should return false if no threshold expression is found with correct type and refId', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [dataQuery];
    const result = areQueriesTransformableToSimpleCondition(dataQueries, [
      reduceExpression,
      { ...thresholdExpression, refId: 'hello' },
    ]);
    expect(result).toBe(false);
  });

  it('should return false if reduceExpression settings mode is not ReducerMode.Strict', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [dataQuery];
    const transformedReduceExpression = produce(reduceExpression, (draft) => {
      draft.model.settings = { mode: ReducerMode.DropNonNumbers };
    });

    const result = areQueriesTransformableToSimpleCondition(dataQueries, [
      transformedReduceExpression,
      thresholdExpression,
    ]);
    expect(result).toBe(false);
  });

  it('should return false if thresholdExpression unloadEvaluator has a value', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [dataQuery];

    const transformedThresholdExpression = produce(thresholdExpression, (draft) => {
      draft.model.conditions = [
        {
          evaluator: { params: [1], type: EvalFunction.IsAbove },
          unloadEvaluator: { params: [1], type: EvalFunction.IsAbove },
          query: { params: ['A'] },
          reducer: { params: [], type: 'avg' },
          type: 'query',
        },
      ];
    });
    const result = areQueriesTransformableToSimpleCondition(dataQueries, [
      reduceExpression,
      transformedThresholdExpression,
    ]);
    expect(result).toBe(false);
  });
  it('should return true when all conditions are met', () => {
    const dataQueries: Array<AlertQuery<AlertDataQuery | ExpressionQuery>> = [dataQuery];
    const result = areQueriesTransformableToSimpleCondition(dataQueries, expressionQueries);
    expect(result).toBe(true);
  });
});
