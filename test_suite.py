import sys
from typing import Any, Callable


def test_function(
    expected_result: Any,
    result_evaluation_algorithm: Callable[[Any, Any], bool],
    function_to_test: Callable[..., Any],
    *args: Any
):
    print(f"\n\texpected_result = {expected_result} \n\tresult_evaluation_algorithm = {
          result_evaluation_algorithm}\n\tfunction_to_test = {function_to_test}\n\tArgs = {args}\n")

    function_result: Any = function_to_test(*args)
    function_works: bool = result_evaluation_algorithm(
        function_result, expected_result)

    print(f'Passed in function {function_to_test.__name__} with arguments: {
          args}\tResult: {function_result}')

    print(f'Expected result {expected_result}, equality checked by {
          result_evaluation_algorithm.__name__}')

    print(f'Comparing expected_result : function_result\t {
          expected_result} : {function_result}')

    if function_works:
        print(f'{function_to_test.__name__} returned the CORRECT result\tFunction works: {
              function_works}', file=sys.stderr)
    else:
        print(f'{function_to_test.__name__} returned the WRONG result\tFunction works: {
              function_works}', file=sys.stderr)

    return function_works
