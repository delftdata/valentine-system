from flask_wtf import FlaskForm
from wtforms import FileField, BooleanField, FieldList, IntegerField, StringField
from wtforms.validators import DataRequired, Optional


class BaseForm(FlaskForm):
    class Meta:
        csrf = False


class UploadFileToMinioForm(BaseForm):
    resource = FileField('file', validators=[DataRequired()])


class DatasetFabricationForm(BaseForm):
    resource = FileField('csv_file', validators=[DataRequired()])
    json_schema = FileField('json_file', validators=[DataRequired()])
    dataset_group_name = StringField('dataset_group_name', validators=[DataRequired()])
    fabricate_joinable = BooleanField('fabricate_joinable', validators=[Optional()],
                                      default=False)
    joinable_specs = FieldList(BooleanField('joinable_specs', validators=[Optional()]),
                               default=[], min_entries=0, validators=[Optional()])
    joinable_pairs = IntegerField('joinable_pairs', validators=[Optional()])
    fabricate_unionable = BooleanField('fabricate_unionable', validators=[Optional()],
                                       default=False)
    unionable_specs = FieldList(BooleanField('unionable_specs', validators=[Optional()]),
                                default=[], min_entries=0, validators=[Optional()])
    unionable_pairs = IntegerField('unionable_pairs', validators=[Optional()])
    fabricate_view_unionable = BooleanField('fabricate_view_unionable', validators=[Optional()],
                                            default=False)
    view_unionable_specs = FieldList(BooleanField('view_unionable_specs', validators=[Optional()]),
                                     default=[], min_entries=0, validators=[Optional()])
    view_unionable_pairs = IntegerField('view_unionable_pairs', validators=[Optional()])
    fabricate_semantically_joinable = BooleanField('fabricate_semantically_joinable', validators=[Optional()],
                                                   default=False)
    semantically_joinable_specs = FieldList(BooleanField('semantically_joinable_specs', validators=[Optional()]),
                                            default=[], min_entries=0, validators=[Optional()])
    semantically_joinable_pairs = IntegerField('semantically_joinable_pairs', validators=[Optional()])
