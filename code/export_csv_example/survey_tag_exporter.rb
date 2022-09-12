class SurveyTagExporter
  SQL_QUERY_TEMPLATE_PATH = Rails.root.join("./query.sql.erb")
  SQL_QUERY_TEMPLATE = ERB.new(File.read(SQL_QUERY_TEMPLATE_PATH))

  def initialize(survey_group)
    @survey_group = survey_group
    @name = survey_group.name.parameterize
  end

  def export
    pg_conn = ActiveRecord::Base.connection.raw_connection # :: PG::Connection
    query = SQL_QUERY_TEMPLATE.result_with_hash(survey_group_id: @survey_group.id)
    csv_output = ""

    log "Start export CSV"

    # PG::Connection#copy_data can take an optional `decoder` argument.  If
    # `decoder` is not set or is `nil`, the data is returned as a binary string
    # with encoding ASCII::8_BIT, even though the underlying data is UTF-8. We
    # supply an instance of `PG::TextDecode::String` which is a very simple
    # decoder which will just change the encoding of the output to whatever
    # `PG::Connection.internal_encoding` is. This is the behaviour we want because
    # `PG::Connection.internal_encoding` is set to `Encoding::UTF_8` by default.
    #
    pg_result = pg_conn.copy_data(query, PG::TextDecoder::String.new) do
      while row = pg_conn.get_copy_data
        csv_output.concat(row)
      end
    end

    log "End export CSV"

    csv_output
  ensure
    # Clear the memory associated with the PG::Result
    log "Cleaning up PG::Result memory"
    pg_result.clear if pg_result && pg_result.respond_to?(:clear)
  end

  def filename
    "#{Time.zone.today.iso8601}-#{@name}-tags.csv"
  end

  private

  def log(msg)
    Rails.logger.info("SurveyTagExporter: #{msg}")
  end
end
