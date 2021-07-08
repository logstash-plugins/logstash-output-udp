# encoding: utf-8

# @api internal
module LogStash::Outputs::UDP::NumberOrFieldReferenceValidator

  # @override {LogStash::Config::Mixin::validate_value} to handle custom validators
  # @param value [Array<Object>]
  # @param validator [nil,Array,Symbol]
  # @return [Array(true,Object)]: if validation is a success, a tuple containing `true` and the coerced value
  # @return [Array(false,String)]: if validation is a failure, a tuple containing `false` and the failure reason.
  def validate_value(value, validator)
    return super unless validator == :number_or_field_reference

    value = deep_replace(value)
    value = hash_or_array(value)

    maybe_int = value.first.to_s

    if maybe_int.eql? maybe_int.to_i.to_s
      validate_value(value, :number)
    else
      validate_value(value, :field_reference)
    end
  end

end
