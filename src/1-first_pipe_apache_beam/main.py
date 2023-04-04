import re
import logging
import argparse
import apache_beam as beam

from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--input',
        dest='input',
        default='C:/Users/G6367033/OneDrive - Pacífico Compañía de Seguros y Reaseguros/Escritorio/Projects/data/data_input/el_quijote.txt',
        help='Input file to process.'
    )
    parser.add_argument(
        '--output',
        dest='output',
        default='C:/Users/G6367033/OneDrive - Pacífico Compañía de Seguros y Reaseguros/Escritorio/Projects/data/data_output/counting_el_quijote.txt',
        help='Output file to write results to.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(known_args.input)

        counts = (
            lines
            | 'Split' >> (
                beam.FlatMap(
                    lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)
        
        output = counts | 'Format' >> beam.Map(format_result)

        output | WriteToText(known_args.output)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
